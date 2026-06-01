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
package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import java.util.concurrent.Future;

/**
 * SPI for moving completed jobs to archive storage on a retention schedule. Default implementation:
 * {@link run.ratchet.ri.core.internal.DefaultJobArchivingService}.
 *
 * @apiNote Framework SPI consumed by ri.cdi.RatchetLifecycle and by ratchet-testsuite integration
 *     tests. Applications must not implement this interface.
 */
public interface JobArchivingService {

  /** Initializes archive retention settings and schedules the first archive pass when enabled. */
  void init(boolean enabled, long retentionDays, int batchSize, Cron cronExpression);

  /** Submits an archive pass to the job executor without joining the caller's transaction. */
  Future<?> triggerArchiving();

  /** Stops future scheduling for this service. */
  void stop();
}
