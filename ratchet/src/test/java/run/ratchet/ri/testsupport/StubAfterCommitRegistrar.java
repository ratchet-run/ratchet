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
package run.ratchet.ri.testsupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import run.ratchet.spi.AfterCommitRegistrar;

/** Controllable after-commit registrar for RI unit tests. */
public final class StubAfterCommitRegistrar implements AfterCommitRegistrar {

  private final List<Runnable> pendingActions = new ArrayList<>();
  private Outcome outcome = Outcome.NO_ACTIVE_TRANSACTION;

  public StubAfterCommitRegistrar outcome(Outcome outcome) {
    this.outcome = Objects.requireNonNull(outcome, "outcome");
    return this;
  }

  @Override
  public Outcome registerAfterCommit(Runnable action, String failureDescription) {
    if (outcome == Outcome.REGISTERED) {
      pendingActions.add(action);
    }
    return outcome;
  }

  public int pendingActionCount() {
    return pendingActions.size();
  }

  public void commit() {
    List<Runnable> actions = List.copyOf(pendingActions);
    pendingActions.clear();
    actions.forEach(Runnable::run);
  }

  public void rollBack() {
    pendingActions.clear();
  }
}
