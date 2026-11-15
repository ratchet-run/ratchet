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

import java.util.List;
import java.util.Optional;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.spi.SchedulerLifecycleHook;

/** Configures and reads concurrency limits for named resources. */
public interface ResourceLimits {

  /**
   * Creates the limit for {@code limit.resourceName()}, or replaces every value of an existing
   * limit, including a null description. Validates before writing.
   *
   * <p>Joins the caller's transaction. On SQL stores the limit row stays locked until commit,
   * blocking permit acquires for that resource. Call this outside long transactions. On MongoDB the
   * change applies at once, and rollback does not undo it.
   *
   * <p>Lowering a limit keeps permits already held. New acquires wait until held permits drop below
   * it. Raising a limit wakes waiting jobs after commit. A new retry delay applies from each job's
   * next miss.
   *
   * <p>Set limits at start-up, for example from {@link SchedulerLifecycleHook#beforeStart()},
   * before submitting any job that names the resource. {@code submit()} throws {@link
   * ResourceNotConfiguredException} for a resource with no limit.
   *
   * @throws IllegalArgumentException for a null limit or a value outside the rules on {@link
   *     ResourceLimit}
   * @throws UnsupportedOperationException when the store lacks the {@code ResourcePermitStore}
   *     capability
   */
  void setResourceLimit(ResourceLimit limit);

  /**
   * Reads one limit. Null or blank returns empty. The name is trimmed. Returns empty when the store
   * lacks the {@code ResourcePermitStore} capability. This operation is read only.
   *
   * <p>Name matching follows the store. MySQL, and SQL Server with its usual collation, ignore
   * case.
   *
   * @throws IllegalArgumentException for a name over 100 characters or outside printable ASCII
   */
  Optional<ResourceLimit> findResourceLimit(String resourceName);

  /**
   * Returns every limit, sorted by name. Returns empty when the store lacks the {@code
   * ResourcePermitStore} capability. Meant for a small number of resources.
   */
  List<ResourceLimit> listResourceLimits();
}
