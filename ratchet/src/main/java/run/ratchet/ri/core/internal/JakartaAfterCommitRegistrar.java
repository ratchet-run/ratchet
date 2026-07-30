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
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import run.ratchet.spi.AfterCommitRegistrar;

/** Jakarta Transactions implementation of {@link AfterCommitRegistrar}. */
@ApplicationScoped
public class JakartaAfterCommitRegistrar implements AfterCommitRegistrar {

  private static final Logger log = Logger.getLogger(JakartaAfterCommitRegistrar.class);

  private volatile TransactionSynchronizationRegistry txRegistry;
  private volatile boolean lookupAttempted;

  public JakartaAfterCommitRegistrar() {}

  JakartaAfterCommitRegistrar(TransactionSynchronizationRegistry txRegistry) {
    this.txRegistry = txRegistry;
    this.lookupAttempted = true;
  }

  @Override
  public Outcome registerAfterCommit(Runnable action, String failureDescription) {
    TransactionSynchronizationRegistry registry = resolveTxRegistry();
    if (registry == null) {
      return Outcome.NO_ACTIVE_TRANSACTION;
    }

    try {
      int transactionStatus = registry.getTransactionStatus();
      if (transactionStatus == Status.STATUS_NO_TRANSACTION) {
        return Outcome.NO_ACTIVE_TRANSACTION;
      }
      if (transactionStatus != Status.STATUS_ACTIVE) {
        log.warnf(
            failureDescription,
            "transaction status " + transactionStatus + " does not allow registration");
        return Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
      }
      registry.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {
              // no-op
            }

            @Override
            public void afterCompletion(int status) {
              if (status == Status.STATUS_COMMITTED) {
                action.run();
              }
            }
          });
      return Outcome.REGISTERED;
    } catch (Exception e) {
      log.warnf(e, failureDescription, e.getMessage());
      return Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
    }
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    if (!lookupAttempted) {
      synchronized (this) {
        if (!lookupAttempted) {
          txRegistry = lookupTxRegistry();
          lookupAttempted = true;
        }
      }
    }
    return txRegistry;
  }

  TransactionSynchronizationRegistry lookupTxRegistry() {
    try {
      return InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
    } catch (NamingException e) {
      log.debugf(
          "TransactionSynchronizationRegistry lookup unavailable; using immediate fallback: %s",
          e.getMessage());
      return null;
    }
  }
}
