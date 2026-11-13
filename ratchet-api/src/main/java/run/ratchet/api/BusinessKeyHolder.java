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

import java.util.Objects;
import java.util.UUID;

/**
 * The holder of a business key: a live job, a recurring master, or a cancelled job whose running
 * body has not ended yet.
 *
 * @param id the job id or recurring master id. {@link JobSchedulerService#cancelJob(UUID)} cancels
 *     either.
 * @param type {@link JobType#RECURRING} for a recurring master. Otherwise the job's public type,
 *     which is {@link JobType#SINGLE} for a job from {@link JobBuilder}.
 * @param status the job's live status: PENDING, RUNNING, PAUSED or WAITING. A recurring master is
 *     PAUSED when paused and PENDING otherwise, as in {@link JobQueryService#getRecurringMasters}.
 *     CANCELED for a cancelled job that still holds the key, as described in {@link
 *     JobSchedulerService#cancelJob(UUID)}. Never null, SUCCEEDED or FAILED.
 */
public record BusinessKeyHolder(UUID id, JobType type, JobStatus status) {
  public BusinessKeyHolder {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(status, "status");
  }
}
