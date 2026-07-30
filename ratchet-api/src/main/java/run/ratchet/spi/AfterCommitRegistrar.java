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
 * Defers an action until the current transaction commits without allowing an uncertain transaction
 * state to publish the action early.
 *
 * <p>A missing transaction and a transaction manager reporting no current transaction produce
 * {@link Outcome#NO_ACTIVE_TRANSACTION}, requiring the caller to execute the action inline itself.
 * An active transaction produces {@link Outcome#REGISTERED} when synchronization registration
 * succeeds; the registrar then runs the action only after a successful commit and discards it on
 * rollback. Any other transaction state, status lookup failure, or synchronization registration
 * failure produces {@link Outcome#ACTIVE_TRANSACTION_REGISTRATION_FAILED}; the action is dropped
 * and a warning is logged, so callers must publish nothing.
 */
@Incubating
public interface AfterCommitRegistrar {

  /**
   * Registers an action for post-commit execution.
   *
   * @param action action to run after a successful commit
   * @param failureDescription caller-context warning message used when registration fails
   * @return registration outcome governing whether the caller may run the action inline
   */
  Outcome registerAfterCommit(Runnable action, String failureDescription);

  /** Outcome of attempting to defer an action until the current transaction commits. */
  enum Outcome {
    /** No transaction is active; the caller must run the action inline itself. */
    NO_ACTIVE_TRANSACTION,

    /** The action will run only after a successful commit and will be discarded on rollback. */
    REGISTERED,

    /** Registration failed; the action was dropped and a warning was logged. */
    ACTIVE_TRANSACTION_REGISTRATION_FAILED
  }
}
