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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import run.ratchet.spi.AfterCommitRegistrar;

/** Spring transaction-synchronization adapter for Ratchet's after-commit publication seam. */
public final class SpringAfterCommitRegistrar implements AfterCommitRegistrar {

  private static final Log log = LogFactory.getLog(SpringAfterCommitRegistrar.class);

  @Override
  public Outcome registerAfterCommit(Runnable action, String failureDescription) {
    try {
      if (!TransactionSynchronizationManager.isActualTransactionActive()) {
        return Outcome.NO_ACTIVE_TRANSACTION;
      }
      if (!TransactionSynchronizationManager.isSynchronizationActive()) {
        // An actual transaction without active synchronization (e.g. inside another
        // synchronization's afterCompletion callback) cannot register and must not
        // be treated as "no transaction" — running the action inline would publish
        // before the surrounding transaction's outcome is known.
        log.warn(
            String.format(
                failureDescription,
                "transaction is active but synchronization is not available for registration"));
        return Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
      }

      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              action.run();
            }
          });
      return Outcome.REGISTERED;
    } catch (Exception exception) {
      log.warn(String.format(failureDescription, exception.getMessage()), exception);
      return Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
    }
  }
}
