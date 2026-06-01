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
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobLogEntity;

/** Per-job log storage operations. */
@Incubating
public interface JobLogStore {

  /**
   * Appends one per-job log line.
   *
   * @param log log entity to persist; never {@code null}
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  void appendLog(JobLogEntity log);

  /**
   * Deletes log lines older than the cutoff instant.
   *
   * @param cutoff exclusive upper bound; rows with timestamps before this instant are eligible for
   *     deletion
   * @return number of deleted log rows
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  int purgeLogsOlderThan(Instant cutoff);
}
