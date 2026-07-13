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
import run.ratchet.api.RecurringMisfirePolicy;
import run.ratchet.spi.ProtectedSurface;
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
    boolean encryptedPayload = RowValues.booleanOrFalse(row[19]);
    if (encryptedPayload
        && PayloadEncryptor.argsFlaggedButUnframed(RowValues.stringOrNull(row[11]))) {
      EncryptionIntegrity.flaggedButUnframed(id, ProtectedSurface.PAYLOAD_ARGS);
    }
    JobPayload payload = decryptPayload(row[11], ProtectedSurface.PAYLOAD_ARGS, id);
    JobPayload onSuccess = decryptPayload(row[12], ProtectedSurface.ON_SUCCESS_PAYLOAD, id);
    JobPayload onFailure = decryptPayload(row[13], ProtectedSurface.ON_FAILURE_PAYLOAD, id);
    String businessKey = (String) row[14];
    String resourceName = (String) row[15];
    String executionTarget = (String) row[16];
    Instant createdAt = RowValues.instantOrNull(row[17]);
    String callerPrincipal = (String) row[18];
    RecurringMisfirePolicy misfirePolicy =
        new RecurringMisfirePolicy(
            RecurringMisfirePolicy.Action.valueOf(RowValues.stringOrNull(row[20])),
            ((Number) row[21]).intValue());

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
        callerPrincipal,
        encryptedPayload,
        misfirePolicy);
  }

  /**
   * Encrypts a recurring-master payload template column. The master's row id is the AAD binding, so
   * a template ciphertext cannot be lifted into a regular job row (which binds the job id) or
   * another master; it reuses the per-surface {@link ProtectedSurface} the equivalent live-job
   * column uses. No-op when {@code active} is false, when the payload is {@code null}, or when it
   * carries no arguments.
   */
  public static String encryptPayloadColumn(
      JobPayload payload, boolean active, ProtectedSurface surface, UUID masterId) {
    return PayloadEncryptor.encryptArgs(
        PAYLOAD_CONVERTER.convertToDatabaseColumn(payload),
        active,
        EncryptionTarget.rowBound(surface, masterId));
  }

  private static JobPayload decryptPayload(Object column, ProtectedSurface surface, UUID masterId) {
    String decrypted =
        PayloadEncryptor.decryptArgs(
            RowValues.stringOrNull(column), EncryptionTarget.rowBound(surface, masterId));
    return PAYLOAD_CONVERTER.convertToEntityAttribute(decrypted);
  }
}
