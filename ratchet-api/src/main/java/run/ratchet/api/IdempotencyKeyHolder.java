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
 * The job held by an idempotency key, including after archive or purge.
 *
 * <p>Resubmitting under this key returns {@code id} and runs nothing, for every retention state.
 *
 * @param id the job id in the ledger, also returned by {@link JobBuilder#submit()}
 * @param retention whether the live row, archive row, or only the ledger entry remains
 * @param type the public job type, as in {@link BusinessKeyHolder}; null only for PURGED
 * @param status the live status for LIVE, or terminal final status for ARCHIVED; null only for
 *     PURGED
 */
public record IdempotencyKeyHolder(UUID id, Retention retention, JobType type, JobStatus status) {
  public IdempotencyKeyHolder {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(retention, "retention");
    if (retention == Retention.PURGED) {
      if (type != null || status != null) {
        throw new IllegalArgumentException("PURGED holders must have null type and status");
      }
    } else {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(status, "status");
    }
  }

  /** The storage that still holds the job behind the ledger entry. */
  public enum Retention {
    /** The live job row remains. */
    LIVE,
    /** Only the archive row remains. */
    ARCHIVED,
    /** Neither row remains, including when the store has no archive capability. */
    PURGED
  }
}
