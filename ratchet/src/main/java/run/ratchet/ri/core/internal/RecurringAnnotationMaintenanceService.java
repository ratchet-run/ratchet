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

import java.time.Instant;
import java.util.Set;

/**
 * Internal maintenance operations for {@code @Recurring} registration cleanup.
 *
 * <p>Implementations run inside the scheduler's normal registration transaction boundary.
 */
public interface RecurringAnnotationMaintenanceService {

  /**
   * Cancels recurring annotation jobs that were previously registered but were not seen during the
   * current startup scan.
   *
   * @implSpec Transaction attribute: REQUIRED. Implementations run inside the registration
   *     transaction that performs the startup annotation cleanup.
   * @param registeredIds active business keys discovered during startup
   * @param nodeStartTime startup timestamp used as a grace cutoff
   * @return the number of canceled orphaned jobs
   */
  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}
