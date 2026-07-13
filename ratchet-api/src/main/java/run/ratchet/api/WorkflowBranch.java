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
package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * A condition-action pair in a job workflow. After a parent job completes, branches are evaluated
 * by priority (lower numbers first), then by builder registration order. The task for the first
 * matching branch is enqueued and every remaining sibling branch is canceled.
 *
 * @param condition condition evaluated after the parent job completes; must not be {@code null}
 * @param task serializable task scheduled when the condition matches; must be a supported Ratchet
 *     job callback
 * @param description optional label for monitoring and debugging
 * @see WorkflowCondition
 * @see JobBuilder#branch(WorkflowCondition, SerializableCheckedRunnable, String)
 */
@Incubating
public record WorkflowBranch(WorkflowCondition condition, Serializable task, String description)
    implements Serializable {

  @Serial private static final long serialVersionUID = -5529141024148855247L;

  public WorkflowBranch {
    Objects.requireNonNull(condition, "condition");
  }

  /** Creates a workflow branch without a description. */
  public WorkflowBranch(WorkflowCondition condition, Serializable task) {
    this(condition, task, null);
  }

  /**
   * @return the priority from the underlying condition (lower numbers are evaluated first)
   */
  public int getPriority() {
    return condition.priority();
  }
}
