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

import io.micronaut.aop.Intercepted;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import io.micronaut.transaction.TransactionOperations;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

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

  private ApplicationContext application() {
    return SqlApplication.start(database, "consumer.circuit-breaker=true");
  }

  @Test
  void opensAndRejectsWithoutCallingTarget() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      assertThat(service).isInstanceOf(Intercepted.class);
      open(() -> service.call(true));
      assertThrows(CircuitBreakerOpenException.class, () -> service.call(false));
      assertThat(service.calls()).isEqualTo(3);
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
      waitForTrial(() -> service.call(false));
      assertThat(service.call(false)).isEqualTo("ok");
      // Once closed, a failure is admitted instead of being rejected as an exhausted trial.
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
  void classLevelBindingGeneratesProxy() {
    try (var context = application()) {
      var service = context.getBean(ClassProtectedService.class);
      assertThat(service).isInstanceOf(Intercepted.class);
      open(service::call);
      assertThrows(CircuitBreakerOpenException.class, service::call);
    }
  }

  @Test
  void defaultServiceNameUsesDeclaringSimpleNameAndMethod() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      open(service::defaultCall);
      CircuitBreakerOpenException failure =
          assertThrows(CircuitBreakerOpenException.class, service::defaultCall);
      assertThat(failure).hasMessageContaining("ProtectedService.defaultCall");
    }
  }

  @Test
  void openCircuitRejectsBeforeTransactionAdvice() {
    try (var context = application()) {
      var service = context.getBean(ProtectedService.class);
      var probe = context.getBean(CircuitBreakerTransactionProbe.class);
      var transactions = context.getBean(TransactionOperations.class);
      open(service::transactionalCall);
      // Positive control: the target observed a transaction and the downstream advice ran.
      assertThat(service.transactionalCalls()).isEqualTo(3);
      assertThat(probe.entries()).isEqualTo(3);
      assertThat(transactions.findTransactionStatus()).isEmpty();
      assertThrows(CircuitBreakerOpenException.class, service::transactionalCall);
      assertThat(probe.entries()).isEqualTo(3);
      assertThat(service.transactionalCalls()).isEqualTo(3);
      assertThat(transactions.findTransactionStatus()).isEmpty();
    }
  }

  private static void open(Runnable call) {
    for (int i = 0; i < 3; i++) assertThrows(IllegalStateException.class, call::run);
  }

  private static void waitForTrial(Runnable call) {
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertDoesNotThrow(call::run));
  }

  @Singleton
  @Primary
  @Requires(property = "consumer.circuit-breaker", value = "true")
  public static class TestConfig implements CircuitBreakerConfigProvider {
    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
      return new CircuitBreakerConfig(50, 20, 1000, 2, 3);
    }
  }

  @Singleton
  @Requires(property = "consumer.circuit-breaker", value = "true")
  public static class ProtectedService {
    private final TransactionOperations<EntityManager> transactions;
    private int calls;
    private int transactionalCalls;

    public ProtectedService(TransactionOperations<EntityManager> transactions) {
      this.transactions = transactions;
    }

    @CircuitBreakerProtected(service = "consumer-service")
    public String call(boolean fail) {
      calls++;
      if (fail) throw new IllegalStateException("failure");
      return "ok";
    }

    public int calls() {
      return calls;
    }

    public int transactionalCalls() {
      return transactionalCalls;
    }

    @CircuitBreakerProtected(service = "ignored", ignoreExceptions = IllegalArgumentException.class)
    public void ignored() {
      throw new IllegalArgumentException("ignored");
    }

    @CircuitBreakerProtected
    public void defaultCall() {
      throw new IllegalStateException("default");
    }

    @Transactional
    @CircuitBreakerProtected(service = "transactional")
    public void transactionalCall() {
      assertThat(transactions.findTransactionStatus()).isPresent();
      transactionalCalls++;
      throw new IllegalStateException("transactional");
    }
  }

  @Singleton
  @Requires(property = "consumer.circuit-breaker", value = "true")
  @CircuitBreakerProtected(service = "class-service")
  public static class ClassProtectedService {
    public void call() {
      throw new IllegalStateException("class");
    }
  }
}
