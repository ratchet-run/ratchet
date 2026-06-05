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
package run.ratchet.store.context;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.util.EncryptionIntegrity;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.JobHydrationSupport;
import run.ratchet.store.util.PayloadEncryptor;
import run.ratchet.store.util.RowValues;

/**
 * Shared row-to-{@link JobEntity} hydration for the JDBC dialects.
 *
 * <p>Holds the 52 positional column indexes and the full {@link #hydrateRow(Object[])} setter body
 * that both SQL stores share. Each dialect supplies only its own SELECT projection (PostgreSQL adds
 * {@code ::text} casts on JSON columns) and a dialect label for hydration error messages.
 *
 * <p>The column order encoded by the {@code IDX_*} constants must match every dialect's projection.
 */
public abstract class AbstractJobRowMapper {

  public static final int HYDRATION_COL_COUNT = 53;
  public static final int IDX_Q_STATUS = 34;

  protected static final int IDX_JOB_ID = 0;
  protected static final int IDX_JOB_TYPE = 1;
  protected static final int IDX_PRIORITY = 2;
  protected static final int IDX_MAX_RETRIES = 3;
  protected static final int IDX_BACKOFF_POLICY = 4;
  protected static final int IDX_BACKOFF_PARAM_MS = 5;
  protected static final int IDX_TIMEOUT_SEC = 6;
  protected static final int IDX_CRON_EXPR = 7;
  protected static final int IDX_ZONE_ID = 8;
  protected static final int IDX_PAYLOAD = 9;
  protected static final int IDX_PARAMS = 10;
  protected static final int IDX_TARGET_CLASS = 11;
  protected static final int IDX_METHOD_NAME = 12;
  protected static final int IDX_IDEMPOTENCY_KEY = 13;
  protected static final int IDX_BUSINESS_KEY = 14;
  protected static final int IDX_RESOURCE_NAME = 15;
  protected static final int IDX_ON_SUCCESS = 16;
  protected static final int IDX_ON_FAILURE = 17;
  protected static final int IDX_DEPENDS_ON = 18;
  protected static final int IDX_SUPERSEDED_BY = 19;
  protected static final int IDX_CREATED_AT = 20;
  protected static final int IDX_CALLER_PRINCIPAL = 21;
  protected static final int IDX_TERMINAL_STATUS = 22;
  protected static final int IDX_TERMINAL_ERROR = 23;
  protected static final int IDX_TOTAL_ATTEMPTS = 24;
  protected static final int IDX_TERMINATED_AT = 25;
  protected static final int IDX_EXEC_START = 26;
  protected static final int IDX_EXEC_END = 27;
  protected static final int IDX_EXEC_DURATION = 28;
  protected static final int IDX_QUEUE_WAIT = 29;
  protected static final int IDX_JOB_RESULT = 30;
  protected static final int IDX_RESULT_TYPE = 31;
  protected static final int IDX_TRACE_CONTEXT = 32;
  protected static final int IDX_RECURRING_MASTER_ID = 33;
  protected static final int IDX_Q_SCHEDULED_TIME = 35;
  protected static final int IDX_Q_ATTEMPTS = 36;
  protected static final int IDX_Q_PICKED_BY = 37;
  protected static final int IDX_Q_PICKED_AT = 38;
  protected static final int IDX_Q_PAUSED = 39;
  protected static final int IDX_Q_LAST_ERROR = 40;
  protected static final int IDX_Q_VERSION = 41;
  protected static final int IDX_Q_UPDATED_AT = 42;
  protected static final int IDX_Q_SIGNAL_KEY = 43;
  protected static final int IDX_Q_SIGNAL_TIMEOUT = 44;
  protected static final int IDX_Q_SIGNAL_PAYLOAD = 45;
  protected static final int IDX_Q_SIGNAL_PAYLOAD_TYPE = 46;
  protected static final int IDX_Q_SIGNAL_OUTCOME = 47;
  protected static final int IDX_Q_SIGNAL_REJECTION_REASON = 48;
  protected static final int IDX_Q_SIGNAL_DELIVERED_AT = 49;
  protected static final int IDX_Q_SIGNAL_DELIVERED_BY = 50;
  protected static final int IDX_Q_SIGNAL_DELIVERY_ID = 51;
  // Appended last so the queue-column indexes above do not shift. encryption_key_id is NOT in the
  // hydration projection — it is read only by the rare key-rotation drain-check query.
  protected static final int IDX_ENCRYPTED_PAYLOAD = 52;

  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();

  private final JobHydrationSupport hydration;

  protected AbstractJobRowMapper(String dialectLabel) {
    this.hydration = new JobHydrationSupport(dialectLabel);
  }

  /**
   * Reconstructs a {@link JobEntity} from one row of the shared hydration projection.
   *
   * <p>Throws {@link IllegalStateException} when a row carries neither a live {@code q.status} nor
   * a terminal status: the terminal transition writes {@code terminal_status} before deleting the
   * queue row in a single transaction, so a committed row always has one or the other. Neither is a
   * corrupt row, surfaced rather than hydrated into an entity with a null status.
   */
  protected JobEntity hydrateRow(Object[] row) {
    if (row == null) {
      return null;
    }
    if (row.length != HYDRATION_COL_COUNT) {
      throw new IllegalStateException(
          "Hydration projection length mismatch: expected "
              + HYDRATION_COL_COUNT
              + " columns, got "
              + row.length);
    }
    JobEntity j = new JobEntity();
    UUID jobId = RowValues.uuidOrNull(row[IDX_JOB_ID]);
    j.setId(jobId);
    // Surfaces are decrypted in place; decryption is marker-driven, so an unencrypted (unframed)
    // value passes through unchanged regardless of the per-row flag. The flag is hydrated below for
    // re-write paths (e.g. result persistence) and integrity tooling.
    j.setEncryptedPayload(RowValues.booleanOrFalse(row[IDX_ENCRYPTED_PAYLOAD]));
    j.setJobType(enumValue(row, IDX_JOB_TYPE, "job_type", JobExecutionType.class));
    j.setPriority(
        RowValues.safeJobPriority(requiredNumber(row, IDX_PRIORITY, "priority").intValue()));
    j.setMaxRetries(requiredNumber(row, IDX_MAX_RETRIES, "max_retries").intValue());
    j.setBackoffPolicy(enumValue(row, IDX_BACKOFF_POLICY, "backoff_policy", BackoffPolicy.class));
    j.setBackoffParamMs(requiredNumber(row, IDX_BACKOFF_PARAM_MS, "backoff_param_ms").intValue());
    j.setTimeoutSec(requiredNumber(row, IDX_TIMEOUT_SEC, "timeout_sec").intValue());
    j.setCronExpr((String) row[IDX_CRON_EXPR]);
    j.setZoneId((String) row[IDX_ZONE_ID]);
    String rawPayload = RowValues.stringOrNull(row[IDX_PAYLOAD]);
    if (j.isEncryptedPayload() && PayloadEncryptor.argsFlaggedButUnframed(rawPayload)) {
      EncryptionIntegrity.flaggedButUnframed(jobId, ProtectedSurface.PAYLOAD_ARGS);
    }
    j.setPayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(
            PayloadEncryptor.decryptArgs(
                rawPayload, EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, jobId))));
    j.setParams(
        JSON_MAP_CONVERTER.convertToEntityAttribute(
            PayloadEncryptor.decryptParamMap(
                RowValues.stringOrNull(row[IDX_PARAMS]),
                EncryptionTarget.rowBound(ProtectedSurface.PARAM_VALUE, jobId))));
    j.setTargetClass((String) row[IDX_TARGET_CLASS]);
    j.setMethodName((String) row[IDX_METHOD_NAME]);
    j.setIdempotencyKey((String) row[IDX_IDEMPOTENCY_KEY]);
    j.setBusinessKey((String) row[IDX_BUSINESS_KEY]);
    j.setResourceName((String) row[IDX_RESOURCE_NAME]);
    j.setOnSuccessPayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(
            PayloadEncryptor.decryptArgs(
                RowValues.stringOrNull(row[IDX_ON_SUCCESS]),
                EncryptionTarget.rowBound(ProtectedSurface.ON_SUCCESS_PAYLOAD, jobId))));
    j.setOnFailurePayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(
            PayloadEncryptor.decryptArgs(
                RowValues.stringOrNull(row[IDX_ON_FAILURE]),
                EncryptionTarget.rowBound(ProtectedSurface.ON_FAILURE_PAYLOAD, jobId))));
    j.setDependsOn(RowValues.uuidOrNull(row[IDX_DEPENDS_ON]));
    j.setSupersededBy(RowValues.uuidOrNull(row[IDX_SUPERSEDED_BY]));
    j.setCreatedAt(RowValues.instantOrNull(row[IDX_CREATED_AT]));
    j.setCallerPrincipal((String) row[IDX_CALLER_PRINCIPAL]);

    JobStatus terminal =
        enumValueOrNull(row, IDX_TERMINAL_STATUS, "terminal_status", JobStatus.class);
    j.setTerminalStatus(terminal);

    j.setExecutionStartTime(RowValues.instantOrNull(row[IDX_EXEC_START]));
    j.setExecutionEndTime(RowValues.instantOrNull(row[IDX_EXEC_END]));
    j.setExecutionDurationMs(RowValues.longOrNull(row[IDX_EXEC_DURATION]));
    j.setQueueWaitMs(RowValues.longOrNull(row[IDX_QUEUE_WAIT]));
    j.setJobResult(RowValues.stringOrNull(row[IDX_JOB_RESULT]));
    j.setResultType((String) row[IDX_RESULT_TYPE]);
    j.setTraceContext(
        JSON_MAP_CONVERTER.convertToEntityAttribute(
            RowValues.stringOrNull(row[IDX_TRACE_CONTEXT])));
    j.setRecurringMasterId(RowValues.uuidOrNull(row[IDX_RECURRING_MASTER_ID]));
    j.setSignalKey((String) row[IDX_Q_SIGNAL_KEY]);
    j.setSignalTimeout(RowValues.instantOrNull(row[IDX_Q_SIGNAL_TIMEOUT]));
    j.setSignalPayload(RowValues.stringOrNull(row[IDX_Q_SIGNAL_PAYLOAD]));
    j.setSignalPayloadType(RowValues.stringOrNull(row[IDX_Q_SIGNAL_PAYLOAD_TYPE]));
    j.setSignalOutcome(RowValues.stringOrNull(row[IDX_Q_SIGNAL_OUTCOME]));
    j.setSignalRejectionReason(RowValues.stringOrNull(row[IDX_Q_SIGNAL_REJECTION_REASON]));
    j.setSignalDeliveredAt(RowValues.instantOrNull(row[IDX_Q_SIGNAL_DELIVERED_AT]));
    j.setSignalDeliveredBy((String) row[IDX_Q_SIGNAL_DELIVERED_BY]);
    j.setSignalDeliveryId(RowValues.stringOrNull(row[IDX_Q_SIGNAL_DELIVERY_ID]));

    JobStatus live = enumValueOrNull(row, IDX_Q_STATUS, "q.status", JobStatus.class);

    JobStatus resolved;
    if (live != null) {
      resolved = live;
    } else if (terminal != null) {
      resolved = terminal;
    } else {
      throw new IllegalStateException("Job " + j.getId() + " has no live or terminal status");
    }
    j.setStatus(resolved);

    if (live != null) {
      j.setScheduledTime(RowValues.instantOrNull(row[IDX_Q_SCHEDULED_TIME]));
      j.setAttempts(requiredNumber(row, IDX_Q_ATTEMPTS, "q.attempts").intValue());
      j.setPickedBy((String) row[IDX_Q_PICKED_BY]);
      j.setPickedAt(RowValues.instantOrNull(row[IDX_Q_PICKED_AT]));
      j.setPausedFromStatus(
          enumValueOrNull(row, IDX_Q_PAUSED, "q.paused_from_status", JobStatus.class));
      j.setLastError(RowValues.stringOrNull(row[IDX_Q_LAST_ERROR]));
      j.setVersion(requiredNumber(row, IDX_Q_VERSION, "q.version").intValue());
      // A live row's terminated_at is always null, so the updatedAt fallback resolves to createdAt.
      Instant updatedAt = RowValues.instantOrNull(row[IDX_Q_UPDATED_AT]);
      j.setUpdatedAt(updatedAt != null ? updatedAt : j.getCreatedAt());
    } else {
      Number ta = numberOrNull(row, IDX_TOTAL_ATTEMPTS, "total_attempts");
      j.setAttempts(ta != null ? ta.intValue() : 0);
      j.setLastError(RowValues.stringOrNull(row[IDX_TERMINAL_ERROR]));
      j.setVersion(0);
      Instant fallbackSched = RowValues.instantOrNull(row[IDX_EXEC_START]);
      if (fallbackSched == null) {
        fallbackSched = RowValues.instantOrNull(row[IDX_CREATED_AT]);
      }
      j.setScheduledTime(fallbackSched);
      Instant updatedAt = RowValues.instantOrNull(row[IDX_TERMINATED_AT]);
      j.setUpdatedAt(updatedAt != null ? updatedAt : j.getCreatedAt());
    }
    return j;
  }

  private <E extends Enum<E>> E enumValue(
      Object[] row, int index, String column, Class<E> enumType) {
    return hydration.enumValue(row, index, column, enumType);
  }

  private <E extends Enum<E>> E enumValueOrNull(
      Object[] row, int index, String column, Class<E> enumType) {
    return hydration.enumValueOrNull(row, index, column, enumType);
  }

  private Number requiredNumber(Object[] row, int index, String column) {
    return hydration.requiredNumber(row, index, column);
  }

  private Number numberOrNull(Object[] row, int index, String column) {
    return hydration.numberOrNull(row, index, column);
  }
}
