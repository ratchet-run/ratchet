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

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobExecutionEntity;

/** Execution history tracking operations. */
@Incubating
public interface ExecutionStore {

  int DEFAULT_PAGE_LIMIT = 100;

  /** Saves one execution record. Transaction attribute: {@code REQUIRED}. */
  JobExecutionEntity saveExecution(JobExecutionEntity execution);

  /**
   * Returns a page of execution records for a job, ordered by attempt ascending.
   *
   * @param limit maximum number of rows to return; {@code 0} returns an empty page
   * @param offset number of matching rows to skip
   *     <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobExecutionEntity> findExecutionsByJobId(UUID jobId, int limit, int offset);

  /** Finds the latest execution for one job. Transaction attribute: {@code SUPPORTS}. */
  Optional<JobExecutionEntity> findLatestExecution(UUID jobId);

  /** Counts execution attempts for one job. Transaction attribute: {@code SUPPORTS}. */
  int countExecutionAttempts(UUID jobId);
}
