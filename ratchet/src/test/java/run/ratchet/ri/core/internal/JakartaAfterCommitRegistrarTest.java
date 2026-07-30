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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.AfterCommitRegistrar.Outcome;

@ExtendWith(MockitoExtension.class)
class JakartaAfterCommitRegistrarTest {

  @Mock private TransactionSynchronizationRegistry txRegistry;

  @Test
  void reportsNoActiveTransactionWithoutRegistry() {
    AtomicInteger actions = new AtomicInteger();

    Outcome outcome =
        new JakartaAfterCommitRegistrar(null).registerAfterCommit(actions::incrementAndGet, "%s");

    assertEquals(Outcome.NO_ACTIVE_TRANSACTION, outcome);
    assertEquals(0, actions.get());
  }

  @Test
  void failedDefaultLookupIsCachedAsNoRegistry() {
    AtomicInteger lookups = new AtomicInteger();
    JakartaAfterCommitRegistrar registrar =
        new JakartaAfterCommitRegistrar() {
          @Override
          TransactionSynchronizationRegistry lookupTxRegistry() {
            lookups.incrementAndGet();
            return null;
          }
        };

    assertEquals(Outcome.NO_ACTIVE_TRANSACTION, registrar.registerAfterCommit(() -> {}, "%s"));
    assertEquals(Outcome.NO_ACTIVE_TRANSACTION, registrar.registerAfterCommit(() -> {}, "%s"));
    assertEquals(1, lookups.get());
  }

  @Test
  void reportsNoActiveTransactionWhenRegistryHasNoTransaction() {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);

    Outcome outcome =
        new JakartaAfterCommitRegistrar(txRegistry).registerAfterCommit(() -> {}, "%s");

    assertEquals(Outcome.NO_ACTIVE_TRANSACTION, outcome);
    verify(txRegistry, never()).registerInterposedSynchronization(any());
  }

  @Test
  void registersActiveTransactionAndRunsOnlyAfterCommit() {
    AtomicInteger actions = new AtomicInteger();
    AtomicReference<Synchronization> synchronization = new AtomicReference<>();
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doAnswer(
            invocation -> {
              synchronization.set(invocation.getArgument(0));
              return null;
            })
        .when(txRegistry)
        .registerInterposedSynchronization(any());

    Outcome outcome =
        new JakartaAfterCommitRegistrar(txRegistry)
            .registerAfterCommit(actions::incrementAndGet, "%s");

    assertEquals(Outcome.REGISTERED, outcome);
    assertEquals(0, actions.get());

    synchronization.get().afterCompletion(Status.STATUS_COMMITTED);

    assertEquals(1, actions.get());
  }

  @Test
  void registeredActionIsDiscardedOnRollback() {
    AtomicInteger actions = new AtomicInteger();
    AtomicReference<Synchronization> synchronization = new AtomicReference<>();
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doAnswer(
            invocation -> {
              synchronization.set(invocation.getArgument(0));
              return null;
            })
        .when(txRegistry)
        .registerInterposedSynchronization(any());

    Outcome outcome =
        new JakartaAfterCommitRegistrar(txRegistry)
            .registerAfterCommit(actions::incrementAndGet, "%s");

    assertEquals(Outcome.REGISTERED, outcome);

    synchronization.get().afterCompletion(Status.STATUS_ROLLEDBACK);

    assertEquals(0, actions.get());
  }

  @Test
  void rejectsTransactionStateThatCannotRegister() {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_MARKED_ROLLBACK);

    Outcome outcome =
        new JakartaAfterCommitRegistrar(txRegistry).registerAfterCommit(() -> {}, "%s");

    assertEquals(Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED, outcome);
    verify(txRegistry, never()).registerInterposedSynchronization(any());
  }

  @Test
  void reportsFailureWhenActiveRegistrationThrows() {
    AtomicInteger actions = new AtomicInteger();
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doThrow(new IllegalStateException("boom"))
        .when(txRegistry)
        .registerInterposedSynchronization(any());

    Outcome outcome =
        new JakartaAfterCommitRegistrar(txRegistry)
            .registerAfterCommit(actions::incrementAndGet, "%s");

    assertEquals(Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED, outcome);
    assertEquals(0, actions.get());
  }
}
