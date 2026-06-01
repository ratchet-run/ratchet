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
package run.ratchet.store.util;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringJobDefinition;

/**
 * Shared hydration of {@link RecurringJobDefinition} from a {@code scheduler_recurring_job}
 * projection. The column order matches {@code SELECT_COLUMNS} in the per-store recurring
 * operations.
 */
public final class RecurringJobRows {

  private static final JobPayloadConverter PAYLOAD_CONVERTER = new JobPayloadConverter();

  private RecurringJobRows() {}

  /**
   * Maps a single recurring-master row. Column coercions go through {@link RowValues} so the same
   * projection hydrates identically across SQL dialects (binary vs native UUID, tinyint vs native
   * boolean, text vs native JSON columns).
   */
  public static RecurringJobDefinition hydrate(Object[] row) {
    UUID id = RowValues.uuidOrNull(row[0]);
    int priority = ((Number) row[1]).intValue();
    int maxRetries = ((Number) row[2]).intValue();
    BackoffPolicy backoffPolicy = BackoffPolicy.valueOf((String) row[3]);
    int backoffParamMs = ((Number) row[4]).intValue();
    int timeoutSec = ((Number) row[5]).intValue();
    String cronExpr = (String) row[6];
    String zoneId = (String) row[7];
    Instant nextFire = RowValues.instantOrNull(row[8]);
    boolean isPaused = RowValues.booleanOrFalse(row[9]);
    Instant pausedAt = RowValues.instantOrNull(row[10]);
    JobPayload payload =
        PAYLOAD_CONVERTER.convertToEntityAttribute(RowValues.stringOrNull(row[11]));
    JobPayload onSuccess =
        PAYLOAD_CONVERTER.convertToEntityAttribute(RowValues.stringOrNull(row[12]));
    JobPayload onFailure =
        PAYLOAD_CONVERTER.convertToEntityAttribute(RowValues.stringOrNull(row[13]));
    String businessKey = (String) row[14];
    String resourceName = (String) row[15];
    String executionTarget = (String) row[16];
    Instant createdAt = RowValues.instantOrNull(row[17]);
    String callerPrincipal = (String) row[18];

    return new RecurringJobDefinition(
        id,
        cronExpr,
        zoneId,
        nextFire,
        isPaused,
        pausedAt,
        priority,
        maxRetries,
        backoffPolicy,
        backoffParamMs,
        timeoutSec,
        payload,
        onSuccess,
        onFailure,
        businessKey,
        resourceName,
        executionTarget,
        createdAt,
        callerPrincipal);
  }
}
