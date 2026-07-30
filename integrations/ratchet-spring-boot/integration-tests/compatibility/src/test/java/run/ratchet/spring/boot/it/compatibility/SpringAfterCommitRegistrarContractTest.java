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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.spring.boot.autoconfigure.SpringAfterCommitRegistrar;

class SpringAfterCommitRegistrarContractTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class));

  @Test
  void autoConfigurationProvidesTheSpringRegistrarInsteadOfTheJakartaCatalogAdapter() {
    contextRunner.run(
        context -> {
          assertEquals(1, context.getBeansOfType(AfterCommitRegistrar.class).size());
          assertInstanceOf(
              SpringAfterCommitRegistrar.class, context.getBean(AfterCommitRegistrar.class));
        });
  }

  @Test
  void applicationRegistrarOverridesTheAutoConfiguredAdapter() {
    AfterCommitRegistrar override =
        (action, failureDescription) -> AfterCommitRegistrar.Outcome.NO_ACTIVE_TRANSACTION;

    contextRunner
        .withBean(AfterCommitRegistrar.class, () -> override)
        .run(
            context -> {
              assertEquals(1, context.getBeansOfType(AfterCommitRegistrar.class).size());
              assertSame(override, context.getBean(AfterCommitRegistrar.class));
            });
  }

  @Test
  void returnsNoActiveTransactionOutsideATransaction() {
    AtomicBoolean actionRan = new AtomicBoolean();

    AfterCommitRegistrar.Outcome outcome =
        new SpringAfterCommitRegistrar()
            .registerAfterCommit(() -> actionRan.set(true), "registration failed: %s");

    assertEquals(AfterCommitRegistrar.Outcome.NO_ACTIVE_TRANSACTION, outcome);
    assertFalse(actionRan.get());
  }

  @Test
  void registeredActionRunsOnlyAfterCommit() {
    SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager transactionManager =
        new SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager();
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
    SpringAfterCommitRegistrar registrar = new SpringAfterCommitRegistrar();
    AtomicBoolean actionRan = new AtomicBoolean();
    AtomicReference<AfterCommitRegistrar.Outcome> outcome = new AtomicReference<>();

    transactionTemplate.executeWithoutResult(
        status -> {
          outcome.set(
              registrar.registerAfterCommit(() -> actionRan.set(true), "registration failed: %s"));
          assertFalse(actionRan.get());
        });

    assertEquals(AfterCommitRegistrar.Outcome.REGISTERED, outcome.get());
    assertTrue(actionRan.get());
    assertEquals(List.of("BEGIN(REQUIRED)", "COMMIT"), transactionManager.events());
  }

  @Test
  void registeredActionIsDiscardedOnRollback() {
    SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager transactionManager =
        new SpringBootManagedBeanCompatibilityTest.RecordingTransactionManager();
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
    SpringAfterCommitRegistrar registrar = new SpringAfterCommitRegistrar();
    AtomicBoolean actionRan = new AtomicBoolean();
    AtomicReference<AfterCommitRegistrar.Outcome> outcome = new AtomicReference<>();

    transactionTemplate.executeWithoutResult(
        status -> {
          outcome.set(
              registrar.registerAfterCommit(() -> actionRan.set(true), "registration failed: %s"));
          status.setRollbackOnly();
        });

    assertEquals(AfterCommitRegistrar.Outcome.REGISTERED, outcome.get());
    assertFalse(actionRan.get());
    assertEquals(List.of("BEGIN(REQUIRED)", "ROLLBACK"), transactionManager.events());
  }

  @Test
  void registrationFailureDropsTheAction() {
    AtomicBoolean actionRan = new AtomicBoolean();
    SpringAfterCommitRegistrar registrar = new SpringAfterCommitRegistrar();

    try (MockedStatic<TransactionSynchronizationManager> transactionManager =
        mockStatic(TransactionSynchronizationManager.class)) {
      transactionManager
          .when(TransactionSynchronizationManager::isSynchronizationActive)
          .thenReturn(true);
      transactionManager
          .when(TransactionSynchronizationManager::isActualTransactionActive)
          .thenReturn(true);
      transactionManager
          .when(
              () ->
                  TransactionSynchronizationManager.registerSynchronization(
                      any(TransactionSynchronization.class)))
          .thenThrow(new IllegalStateException("registration rejected"));

      AfterCommitRegistrar.Outcome outcome =
          registrar.registerAfterCommit(() -> actionRan.set(true), "registration failed: %s");

      assertEquals(AfterCommitRegistrar.Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED, outcome);
      assertFalse(actionRan.get());
    }
  }
}
