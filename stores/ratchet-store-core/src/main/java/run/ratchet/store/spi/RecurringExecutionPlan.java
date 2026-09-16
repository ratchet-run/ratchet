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
package run.ratchet.store.spi;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobEntity;

/**
 * Children and their master's successor state, committed atomically; null nextFire means exhausted.
 */
@Incubating
public record RecurringExecutionPlan(
    RecurringClaim claim, List<JobEntity> children, Instant nextFire) {
  public RecurringExecutionPlan {
    Objects.requireNonNull(claim, "claim");
    children = List.copyOf(children);
    for (JobEntity child : children) {
      if (!claim.definition().id().equals(child.getRecurringMasterId())) {
        throw new IllegalArgumentException("Recurring child must reference its claimed master");
      }
    }
  }
}
