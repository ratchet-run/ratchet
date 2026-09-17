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
package run.ratchet.spring.boot.autoconfigure;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.spi.AfterCommitRegistrar;

/** Spring transaction-synchronization implementation of Ratchet's after-commit seam. */
public final class SpringAfterCommitRegistrar implements AfterCommitRegistrar {
  private static final Log log = LogFactory.getLog(SpringAfterCommitRegistrar.class);
  private final Supplier<PlatformTransactionManager> transactionManager;

  public SpringAfterCommitRegistrar(Supplier<PlatformTransactionManager> transactionManager) {
    this.transactionManager = Objects.requireNonNull(transactionManager, "transactionManager");
  }

  @Override
  public Result registerAfterCommit(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      return Result.NO_ACTIVE_TRANSACTION;
    }
    if (!TransactionSynchronizationManager.isSynchronizationActive()) {
      return Result.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
    }
    QueueSynchronization actions = queueSynchronization();
    TransactionTemplate boundary = null;
    if (actions == null) {
      // Resolve before registering: ambiguous transaction managers must reject the submission,
      // rather than discover an unusable callback scope only after its transaction has committed.
      PlatformTransactionManager manager;
      try {
        manager = Objects.requireNonNull(transactionManager.get(), "transactionManager");
      } catch (RuntimeException failure) {
        throw new IllegalStateException(
            "Ratchet after-commit callbacks require one default or @Primary"
                + " PlatformTransactionManager",
            failure);
      }
      boundary = new TransactionTemplate(manager);
      boundary.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
    }
    try {
      if (actions == null) {
        actions = new QueueSynchronization(boundary);
        TransactionSynchronizationManager.registerSynchronization(actions);
      }
      if (actions.completed) {
        return Result.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
      }
      actions.actions.add(action);
      return Result.REGISTERED;
    } catch (RuntimeException failure) {
      log.warn("After-commit registration failed; action suppressed", failure);
      return Result.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
    }
  }

  private static QueueSynchronization queueSynchronization() {
    return TransactionSynchronizationManager.getSynchronizations().stream()
        .filter(QueueSynchronization.class::isInstance)
        .map(QueueSynchronization.class::cast)
        .findFirst()
        .orElse(null);
  }

  private static final class QueueSynchronization implements TransactionSynchronization {
    private final List<Runnable> actions = new ArrayList<>();
    private boolean completed;
    private final TransactionTemplate boundary;

    private QueueSynchronization(TransactionTemplate boundary) {
      this.boundary = boundary;
    }

    @Override
    public void afterCompletion(int status) {
      completed = true;
      if (status != STATUS_COMMITTED) {
        actions.clear();
        return;
      }
      List<Runnable> ready = List.copyOf(actions);
      actions.clear();
      for (Runnable callback : ready) {
        try {
          // Spring has not yet unbound the completed transaction's resources at this point.
          // Suspend them so a listener's ordinary REQUIRED work starts and commits a fresh unit.
          boundary.executeWithoutResult(ignored -> callback.run());
        } catch (RuntimeException failure) {
          log.warn("After-commit action failed; continuing remaining actions", failure);
        }
      }
    }
  }
}
