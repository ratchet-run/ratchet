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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.transaction.Status;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class JobTransactionTest {
  @Test
  void rollbackOnlyFollowsRegistryStatus() {
    TransactionSynchronizationRegistry registry = mock(TransactionSynchronizationRegistry.class);
    JobTransaction carrier = new JobTransaction(() -> registry);
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    assertFalse(carrier.isRollbackOnly());
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    assertFalse(carrier.isRollbackOnly());
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_MARKED_ROLLBACK);
    assertTrue(carrier.isRollbackOnly());
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_ROLLING_BACK);
    assertTrue(carrier.isRollbackOnly());
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_ROLLEDBACK);
    assertTrue(carrier.isRollbackOnly());
  }

  @Test
  void rollbackOnlyFailsClosedWithoutRegistry() {
    assertThrows(
        IllegalStateException.class, () -> new JobTransaction(() -> null).isRollbackOnly());
  }

  @Test
  void hasActiveTransactionFailsClosedWithoutRegistry() {
    assertThrows(
        IllegalStateException.class, () -> new JobTransaction(() -> null).hasActiveTransaction());
  }

  @Test
  void registryLookupRunsOnce() {
    AtomicInteger lookups = new AtomicInteger();
    TransactionSynchronizationRegistry registry = mock(TransactionSynchronizationRegistry.class);
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    JobTransaction carrier =
        new JobTransaction(
            () -> {
              lookups.incrementAndGet();
              return registry;
            });
    assertFalse(carrier.hasActiveTransaction());
    assertFalse(carrier.hasActiveTransaction());
    assertEquals(1, lookups.get());
  }

  @Test
  void missingRegistryLookupRunsOnce() {
    AtomicInteger lookups = new AtomicInteger();
    JobTransaction carrier =
        new JobTransaction(
            () -> {
              lookups.incrementAndGet();
              return null;
            });
    assertThrows(IllegalStateException.class, carrier::hasActiveTransaction);
    assertThrows(IllegalStateException.class, carrier::hasActiveTransaction);
    assertEquals(1, lookups.get());
  }

  @Test
  void unitChecksForOpenTransactionBeforeCarrier() {
    TransactionSynchronizationRegistry registry = mock(TransactionSynchronizationRegistry.class);
    JobTransaction transactions = new JobTransaction(() -> registry);
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    assertFalse(transactions.hasActiveTransaction());
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    assertTrue(transactions.hasActiveTransaction());
    when(registry.getTransactionStatus()).thenReturn(Status.STATUS_MARKED_ROLLBACK);
    assertTrue(transactions.hasActiveTransaction());
  }

  @Test
  void markerIsReentrantAndClearedOnReturn() throws Exception {
    JobTransaction carrier = new JobTransaction();
    assertFalse(JobTransaction.active());
    assertEquals(
        "done",
        carrier.run(
            () -> {
              assertTrue(JobTransaction.active());
              carrier.run(
                  () -> {
                    assertTrue(JobTransaction.active());
                    return null;
                  });
              assertTrue(JobTransaction.active());
              return "done";
            }));
    assertFalse(JobTransaction.active());
  }

  @Test
  void checkedExceptionIsUnwrappedAndNestedMarkerRestored() throws Exception {
    JobTransaction carrier = new JobTransaction();
    IOException failure = new IOException("body");
    carrier.run(
        () -> {
          assertSame(
              failure,
              assertThrows(
                  IOException.class,
                  () ->
                      carrier.run(
                          () -> {
                            throw failure;
                          })));
          assertTrue(JobTransaction.active());
          return null;
        });
    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                carrier.run(
                    () -> {
                      throw failure;
                    })));
    assertFalse(JobTransaction.active());
    assertThrows(
        AssertionError.class,
        () ->
            carrier.run(
                () -> {
                  throw new AssertionError();
                }));
    assertFalse(JobTransaction.active());
  }
}
