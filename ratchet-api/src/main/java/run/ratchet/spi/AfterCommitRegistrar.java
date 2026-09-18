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
package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Defers actions until the transaction associated with the current thread commits.
 *
 * <p>Implementations must preserve action ordering for registrations in the same transaction. A
 * caller may run its action immediately only when {@link Result#NO_ACTIVE_TRANSACTION} is returned.
 * Both other outcomes require suppression because the registrar either owns the action or cannot
 * determine whether the transaction will commit.
 */
@Incubating
@FunctionalInterface
public interface AfterCommitRegistrar {

  /**
   * Attempts to register an action that runs only after a successful transaction commit.
   *
   * @param action action to defer; must not be {@code null}
   * @return the registration outcome
   */
  Result registerAfterCommit(Runnable action);

  /** Outcome of attempting to defer an action until the current transaction commits. */
  enum Result {
    /** No transaction is active, so the caller may run the action immediately. */
    NO_ACTIVE_TRANSACTION,

    /** The action is registered and will run only if the transaction commits. */
    REGISTERED,

    /** A transaction exists, but its state prevented registration or registration failed. */
    ACTIVE_TRANSACTION_REGISTRATION_FAILED
  }
}
