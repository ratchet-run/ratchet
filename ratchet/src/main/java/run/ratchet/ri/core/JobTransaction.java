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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;

/** Transaction boundary shared by a job body and its success completion. */
@ApplicationScoped
public class JobTransaction {
  private static final ThreadLocal<Integer> DEPTH = new ThreadLocal<>();

  private final Supplier<TransactionSynchronizationRegistry> registrySupplier;
  private TransactionSynchronizationRegistry txRegistry;
  private boolean registryLookedUp;

  public JobTransaction() {
    this.registrySupplier = null;
  }

  public JobTransaction(Supplier<TransactionSynchronizationRegistry> registry) {
    this.registrySupplier = registry;
  }

  /**
   * Returns whether the calling thread already has a transaction, in any state. Units start their
   * own transaction and refuse to run inside one. Looks up the registry at most once per instance.
   *
   * @throws IllegalStateException if no TransactionSynchronizationRegistry is available
   */
  public boolean hasActiveTransaction() {
    TransactionSynchronizationRegistry registry = registry();
    if (registry == null)
      throw new IllegalStateException(
          "cannot tell whether a transaction is open: no TransactionSynchronizationRegistry");
    return registry.getTransactionStatus() != Status.STATUS_NO_TRANSACTION;
  }

  /**
   * Returns whether the transaction on the calling thread is marked for rollback or is already
   * rolling back or rolled back; false when there is no transaction. Ratchet calls this inside
   * {@link #run(Callable)} before it records the result.
   *
   * @throws IllegalStateException if no TransactionSynchronizationRegistry is available
   */
  public boolean isRollbackOnly() {
    TransactionSynchronizationRegistry registry = registry();
    if (registry == null)
      throw new IllegalStateException(
          "cannot tell whether the transaction is marked for rollback: no"
              + " TransactionSynchronizationRegistry");
    return switch (registry.getTransactionStatus()) {
      case Status.STATUS_MARKED_ROLLBACK, Status.STATUS_ROLLING_BACK, Status.STATUS_ROLLEDBACK ->
          true;
      default -> false;
    };
  }

  private synchronized TransactionSynchronizationRegistry registry() {
    if (!registryLookedUp) {
      registryLookedUp = true;
      if (registrySupplier == null) {
        try {
          txRegistry = JakartaAfterCommitRegistrar.lookupTxRegistry();
        } catch (RuntimeException | LinkageError unavailable) {
          txRegistry = null;
        }
      } else {
        txRegistry = registrySupplier.get();
      }
    }
    return txRegistry;
  }

  @Transactional(value = TxType.REQUIRED, rollbackOn = Exception.class)
  public <T> T run(Callable<T> work) throws Exception {
    return insideCarrier(work);
  }

  /** Internal carrier marker, shared by all integration variants. */
  public static boolean active() {
    return DEPTH.get() != null;
  }

  protected static <T> T insideCarrier(Callable<T> work) throws Exception {
    Integer previous = DEPTH.get();
    DEPTH.set(previous == null ? 1 : previous + 1);
    try {
      return work.call();
    } finally {
      if (previous == null) DEPTH.remove();
      else DEPTH.set(previous);
    }
  }
}
