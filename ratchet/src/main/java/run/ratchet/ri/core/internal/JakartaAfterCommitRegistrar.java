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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import run.ratchet.spi.AfterCommitRegistrar;

/** Jakarta Transactions implementation of {@link AfterCommitRegistrar}. */
@ApplicationScoped
public class JakartaAfterCommitRegistrar implements AfterCommitRegistrar {
  private static final String AFTER_COMMIT_ACTIONS_KEY =
      JakartaAfterCommitRegistrar.class.getName() + ".afterCommitActions";
  private static final Logger log = Logger.getLogger(JakartaAfterCommitRegistrar.class);

  private volatile TransactionSynchronizationRegistry txRegistry;

  public JakartaAfterCommitRegistrar() {}

  public JakartaAfterCommitRegistrar(TransactionSynchronizationRegistry txRegistry) {
    this.txRegistry = txRegistry;
  }

  @Override
  public Result registerAfterCommit(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    TransactionSynchronizationRegistry registry = resolveTxRegistry();
    if (registry == null) {
      return Result.NO_ACTIVE_TRANSACTION;
    }

    try {
      int transactionStatus = registry.getTransactionStatus();
      if (transactionStatus == Status.STATUS_NO_TRANSACTION) {
        return Result.NO_ACTIVE_TRANSACTION;
      }
      if (transactionStatus != Status.STATUS_ACTIVE) {
        log.warnf(
            "After-commit registration failed; transaction status %s does not allow registration",
            transactionStatus);
        return Result.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
      }

      // JTA does not promise FIFO delivery between separate synchronizations. Keep Ratchet
      // actions in one transaction-scoped queue so terminal events precede dependent followups.
      synchronized (registry) {
        @SuppressWarnings("unchecked")
        List<Runnable> existing = (List<Runnable>) registry.getResource(AFTER_COMMIT_ACTIONS_KEY);
        List<Runnable> actions = existing;
        if (actions == null) {
          actions = new ArrayList<>();
          List<Runnable> registeredActions = actions;
          registry.registerInterposedSynchronization(
              new Synchronization() {
                @Override
                public void beforeCompletion() {}

                @Override
                public void afterCompletion(int status) {
                  List<Runnable> ready;
                  synchronized (registry) {
                    ready =
                        status == Status.STATUS_COMMITTED
                            ? List.copyOf(registeredActions)
                            : List.of();
                    registeredActions.clear();
                  }
                  for (Runnable callback : ready) {
                    try {
                      callback.run();
                    } catch (RuntimeException failure) {
                      log.warn("After-commit action failed; continuing remaining actions", failure);
                    }
                  }
                }
              });
          // If resource storage fails, the callback retains an empty queue and cannot execute an
          // action whose registration was reported as failed.
          registry.putResource(AFTER_COMMIT_ACTIONS_KEY, actions);
        }
        actions.add(action);
      }
      return Result.REGISTERED;
    } catch (Exception failure) {
      log.warnf(
          failure, "After-commit registration failed; action suppressed: %s", failure.getMessage());
      return Result.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
    }
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry registry = txRegistry;
    if (registry == null) {
      synchronized (this) {
        registry = txRegistry;
        if (registry == null) {
          registry = lookupTxRegistry();
          txRegistry = registry;
        }
      }
    }
    return registry;
  }

  public static TransactionSynchronizationRegistry lookupTxRegistry() {
    return lookupTxRegistry(
        () -> {
          var registry = CDI.current().select(TransactionSynchronizationRegistry.class);
          return registry.isResolvable() ? registry.get() : null;
        });
  }

  static TransactionSynchronizationRegistry lookupTxRegistry(
      Supplier<TransactionSynchronizationRegistry> cdiLookup) {
    try {
      return InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
    } catch (NamingException unavailable) {
      try {
        return cdiLookup.get();
      } catch (IllegalStateException unavailableCdi) {
        log.debugf(
            "TransactionSynchronizationRegistry unavailable through JNDI and CDI: %s",
            unavailableCdi.getMessage());
        return null;
      }
    }
  }
}
