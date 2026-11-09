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

import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;

/** Prevents another attempt after a batch has stopped accepting child work. */
final class BatchRetryGuard {
  private BatchRetryGuard() {}

  static ParentState check(JobCrudStore store, JobEntity job) {
    if (job.getJobType() != JobExecutionType.BATCH_CHILD || job.getDependsOn() == null) {
      return new ParentState(false, null);
    }
    JobStatus status = store.getJobStatus(job.getDependsOn());
    return new ParentState(status != JobStatus.PENDING && status != JobStatus.PAUSED, status);
  }

  record ParentState(boolean closed, JobStatus status) {
    String description() {
      return status == null ? "missing" : status.name();
    }
  }
}
