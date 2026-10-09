/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example.ratchet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import example.ratchet.CircuitBreakerBeanConfiguration.ClassProtectedService;
import example.ratchet.CircuitBreakerBeanConfiguration.InheritedProtectedService;
import example.ratchet.CircuitBreakerBeanConfiguration.ProtectedService;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.consumer.sql.SqlDatabase;

class CircuitBreakerBeanTest {
  private static SqlDatabase database;

  @BeforeAll
  static void startDatabase() {
    database = SqlDatabase.start();
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) database.close();
  }

  private ConfigurableApplicationContext application(String... properties) {
    return new SpringApplicationBuilder(ConsumerApplication.class)
        .sources(TransactionProbeConfiguration.class)
        .profiles("verification")
        .properties(database.properties())
        .properties(properties)
        .run();
  }

  @Test
  void opensAndRejectsWithoutCallingTarget() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      assertThat(AopUtils.isAopProxy(service)).isTrue();
      open(() -> service.call(true));
      assertThrows(CircuitBreakerOpenException.class, () -> service.call(false));
      assertThat(service.calls()).isEqualTo(3);
    }
  }

  @Test
  void worksWhenBootAopIsDisabled() {
    try (var context = application("spring.aop.auto=false")) {
      var service = context.getBean(ProtectedService.class);
      assertThat(AopUtils.isAopProxy(service)).isTrue();
      open(() -> service.call(true));
      assertThrows(CircuitBreakerOpenException.class, () -> service.call(false));
    }
  }

  @Test
  void ignoredExceptionsDoNotOpen() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      for (int i = 0; i < 10; i++) assertThrows(IllegalArgumentException.class, service::ignored);
      assertThat(service.call(false)).isEqualTo("ok");
    }
  }

  @Test
  void halfOpenTrialsCloseTheCircuit() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      open(() -> service.call(true));
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertDoesNotThrow(() -> service.call(false)));
      assertThat(service.call(false)).isEqualTo("ok");
      assertThrows(IllegalStateException.class, () -> service.call(true));
    }
  }

  @Test
  void failedHalfOpenTrialReopens() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      open(() -> service.call(true));
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThrows(IllegalStateException.class, () -> service.call(true)));
      assertThrows(CircuitBreakerOpenException.class, () -> service.call(false));
      assertThat(service.calls()).isEqualTo(4);
    }
  }

  @Test
  void classLevelAndInheritedBindingsCreateProxies() {
    // Each context has an independent registry, so inheritance cannot borrow an already-open
    // circuit.
    try (var context = application()) {
      var service = context.getBean("classProtectedService", ClassProtectedService.class);
      assertThat(AopUtils.isAopProxy(service)).isTrue();
      open(service::call);
      assertThrows(CircuitBreakerOpenException.class, service::call);
    }
    try (var context = application()) {
      var service = context.getBean(InheritedProtectedService.class);
      assertThat(AopUtils.isAopProxy(service)).isTrue();
      open(service::call);
      assertThrows(CircuitBreakerOpenException.class, service::call);
    }
  }

  @Test
  void defaultServiceNameUsesDeclaringSimpleNameAndMethod() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      open(service::defaultCall);
      assertThat(assertThrows(CircuitBreakerOpenException.class, service::defaultCall))
          .hasMessageContaining("ProtectedService.defaultCall");
    }
  }

  @Test
  void openCircuitRejectsBeforeTransactionStarts() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      var manager = context.getBean(CountingTransactions.class);
      int before = manager.starts;
      open(service::transactionalCall);
      assertThat(service.transactionalCalls()).isEqualTo(3);
      assertThat(manager.starts).isEqualTo(before + 3);
      assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
      assertThrows(CircuitBreakerOpenException.class, service::transactionalCall);
      assertThat(manager.starts).isEqualTo(before + 3);
      assertThat(service.transactionalCalls()).isEqualTo(3);
      assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
    }
  }

  private static void open(Runnable call) {
    for (int i = 0; i < 3; i++) assertThrows(IllegalStateException.class, call::run);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class TransactionProbeConfiguration {
    // Boot attaches listener beans to its transaction manager. Ratchet requires the real
    // JpaTransactionManager, so the probe observes it instead of wrapping it.
    @Bean
    CountingTransactions countingTransactions() {
      return new CountingTransactions();
    }
  }

  static class CountingTransactions implements TransactionExecutionListener {
    private final Thread caller = Thread.currentThread();
    private int starts;

    @Override
    public void beforeBegin(TransactionExecution transaction) {
      // Worker transactions must not make the caller-thread ordering assertion race.
      if (Thread.currentThread() == caller) starts++;
    }
  }
}
