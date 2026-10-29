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

import static java.util.Objects.requireNonNull;

import org.jboss.logging.Logger;
import run.ratchet.spi.AfterCommitRegistrar;

/**
 * Applies fail-closed or warn-only policies to deferred actions. API changes fail when registration
 * fails so their transaction rolls back. Engine changes after job execution only warn: rolling back
 * a terminal write makes orphan recovery run the job again.
 */
public final class AfterCommitActions {
  private static final Logger log = Logger.getLogger(AfterCommitActions.class);

  private AfterCommitActions() {}

  /** Runs immediately without a transaction, or fails if registration could lose the action. */
  public static void runOrFail(AfterCommitRegistrar registrar, String what, Runnable action) {
    switch (requireNonNull(
        registrar.registerAfterCommit(action),
        "AfterCommitRegistrar contract requires a non-null result")) {
      case NO_ACTIVE_TRANSACTION -> action.run();
      case REGISTERED -> {}
      case ROLLBACK_ONLY ->
          log.debugf("After-commit %s suppressed; transaction will roll back", what);
      case REGISTRATION_FAILED ->
          throw new IllegalStateException(
              "Could not register "
                  + what
                  + " to run after the current transaction commits; failing so the transaction"
                  + " rolls back instead of committing without it");
    }
  }

  /** Runs immediately without a transaction, or warns if registration could lose the action. */
  public static void runOrWarn(AfterCommitRegistrar registrar, String what, Runnable action) {
    switch (requireNonNull(
        registrar.registerAfterCommit(action),
        "AfterCommitRegistrar contract requires a non-null result")) {
      case NO_ACTIVE_TRANSACTION -> action.run();
      case REGISTERED -> {}
      case ROLLBACK_ONLY ->
          log.debugf("After-commit %s suppressed; transaction will roll back", what);
      case REGISTRATION_FAILED ->
          log.warnf(
              "Could not register %s to run after the current transaction commits; the change"
                  + " commits without it",
              what);
    }
  }
}
