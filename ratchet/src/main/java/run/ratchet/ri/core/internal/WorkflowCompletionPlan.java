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

import java.util.List;
import run.ratchet.store.dto.JobCompletionPlan.DependencyTransition;

/** Store mutations and notifications prepared before committing a terminal job transition. */
public record WorkflowCompletionPlan(
    List<DependencyTransition> dependencies, List<Object> events, boolean newWorkAvailable) {
  public WorkflowCompletionPlan {
    dependencies = List.copyOf(dependencies);
    events = List.copyOf(events);
  }

  public static WorkflowCompletionPlan empty() {
    return new WorkflowCompletionPlan(List.of(), List.of(), false);
  }
}
