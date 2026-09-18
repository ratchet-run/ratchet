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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.Set;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Internal recurring cleanup remains available when an application replaces the public scheduler.
 */
@ApplicationScoped
public class DefaultRecurringAnnotationMaintenanceService
    implements RecurringAnnotationMaintenanceService {
  private final RecurringJobStore recurring;

  protected DefaultRecurringAnnotationMaintenanceService() {
    recurring = null;
  }

  @Inject
  public DefaultRecurringAnnotationMaintenanceService(JobStore store) {
    recurring = store.capability(RecurringJobStore.class).orElse(null);
  }

  @Override
  @Transactional
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return recurring == null
        ? 0
        : recurring.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }
}
