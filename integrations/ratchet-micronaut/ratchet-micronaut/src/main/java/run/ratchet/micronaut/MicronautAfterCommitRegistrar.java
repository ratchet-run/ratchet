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
package run.ratchet.micronaut;

import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.support.TransactionSynchronization;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.spi.AfterCommitRegistrar;

/** Per-transaction FIFO callbacks, suspended from completed transaction resources. */
public final class MicronautAfterCommitRegistrar implements AfterCommitRegistrar {
  private static final Logger LOG = Logger.getLogger(MicronautAfterCommitRegistrar.class.getName());
  private final Supplier<? extends TransactionOperations<?>> operations;
  private final Map<Object, QueueSynchronization> queues = new IdentityHashMap<>();

  public MicronautAfterCommitRegistrar(Supplier<? extends TransactionOperations<?>> operations) {
    this.operations = Objects.requireNonNull(operations, "operations");
  }

  @Override
  public Result registerAfterCommit(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    TransactionOperations<?> manager;
    try {
      manager = operations.get();
    } catch (RuntimeException failure) {
      throw new IllegalStateException(
          "Ratchet after-commit callbacks require one default or @Primary TransactionOperations",
          failure);
    }
    if (manager == null) return Result.NO_ACTIVE_TRANSACTION;
    TransactionStatus<?> status = manager.findTransactionStatus().orElse(null);
    if (status == null) return Result.NO_ACTIVE_TRANSACTION;
    if (!MicronautTransactions.hasActiveTransaction(manager)) return Result.NO_ACTIVE_TRANSACTION;
    if (status.isCompleted()) {
      LOG.warning("After-commit action suppressed; the transaction has already completed");
      return Result.REGISTRATION_FAILED;
    }
    status = MicronautTransactions.rootStatus(status);
    // JDBC leaves getTransaction() null even for a real transaction, and participating JPA
    // statuses need not copy it. Use the shared connection/session identity for the entire unit.
    // The connection identity is stable across JPA REQUIRED participants and distinct for
    // REQUIRES_NEW, on both Micronaut lines; the consumer transaction tests rely on it.
    Object transaction;
    try {
      transaction = status.getConnection();
    } catch (RuntimeException failure) {
      transaction = status.getTransaction();
    }
    if (transaction == null) transaction = status.getTransaction();
    if (transaction == null) {
      LOG.warning("After-commit action suppressed; the transaction has no connection to key it on");
      return Result.REGISTRATION_FAILED;
    }
    synchronized (queues) {
      QueueSynchronization queue = queues.get(transaction);
      if (queue == null) {
        queue = new QueueSynchronization(transaction, manager);
        try {
          status.registerSynchronization(queue);
          queues.put(transaction, queue);
        } catch (RuntimeException failure) {
          LOG.log(Level.WARNING, "After-commit registration failed; action suppressed", failure);
          return Result.REGISTRATION_FAILED;
        }
      }
      if (queue.completed) {
        LOG.warning("After-commit action suppressed; the transaction has already completed");
        return Result.REGISTRATION_FAILED;
      }
      queue.actions.add(action);
      return Result.REGISTERED;
    }
  }

  private final class QueueSynchronization implements TransactionSynchronization {
    private final Object transaction;
    private final TransactionOperations<?> manager;
    private final List<Runnable> actions = new ArrayList<>();
    private boolean completed;

    private QueueSynchronization(Object transaction, TransactionOperations<?> manager) {
      this.transaction = transaction;
      this.manager = manager;
    }

    @Override
    public void afterCompletion(Status status) {
      synchronized (queues) {
        queues.remove(transaction, this);
        completed = true;
        actions.clear();
      }
    }

    // Run before Micronaut's invocation synchronizations restore the previous propagated
    // context. During afterCompletion that restoration can hide a callback's new transaction.
    @Override
    public void afterCommit() {
      List<Runnable> ready;
      synchronized (queues) {
        queues.remove(transaction, this);
        completed = true;
        ready = List.copyOf(actions);
        actions.clear();
      }
      for (Runnable callback : ready) {
        try {
          manager.execute(
              TransactionDefinition.of(Propagation.NOT_SUPPORTED),
              ignored -> {
                callback.run();
                return null;
              });
        } catch (RuntimeException failure) {
          LOG.log(
              Level.WARNING, "After-commit action failed; continuing remaining actions", failure);
        }
      }
    }
  }
}
