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
package run.ratchet.store.postgresql;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.context.AbstractJobRowMapper;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;
import run.ratchet.store.util.RowValues;
import run.ratchet.store.util.StatusClassifier;

/**
 * Hydrates {@link JobEntity} from the hot/cold split schema:
 *
 * <ul>
 *   <li>Cold metadata + terminal fields from {@code scheduler_job} (alias {@code c}).
 *   <li>Live state from {@code scheduler_job_queue} (alias {@code q}) when present.
 * </ul>
 *
 * <p>Shares the column indexes and hydration body with {@link AbstractJobRowMapper}; only the
 * projection differs (PostgreSQL casts JSON columns to {@code ::text}).
 */
final class PostgresqlJobRowMapper extends AbstractJobRowMapper {

  static final int HYDRATION_COL_COUNT = AbstractJobRowMapper.HYDRATION_COL_COUNT;
  static final int IDX_Q_STATUS = AbstractJobRowMapper.IDX_Q_STATUS;
  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();
  private static final PostgresqlJobRowMapper SHARED = new PostgresqlJobRowMapper();

  private PostgresqlJobRowMapper() {
    super("PostgreSQL");
  }

  /**
   * Returns a SELECT projection joining the cold metadata table {@code scheduler_job} (alias {@code
   * c}) with the hot queue table {@code scheduler_job_queue} (alias {@code q}). Callers must
   * include {@code FROM scheduler_job c LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id} in
   * their query. The column order matches {@link AbstractJobRowMapper}'s index constants.
   */
  static String hydrationSelect() {
    // language=PostgreSQL
    return """
        c.job_id, c.job_type, c.priority, c.max_retries, c.backoff_policy,
        c.backoff_param_ms, c.timeout_sec, c.cron_expr, c.zone_id,
        c.payload::text, c.params::text, c.target_class, c.method_name, c.idempotency_key,
        c.business_key, c.resource_name, c.on_success_payload::text,
        c.on_failure_payload::text, c.depends_on, c.superseded_by, c.created_at,
        c.caller_principal, c.terminal_status, c.terminal_error,
        c.total_attempts, c.terminated_at, c.execution_start_time, c.execution_end_time,
        c.execution_duration_ms, c.queue_wait_ms, c.job_result::text, c.result_type,
        c.trace_context::text, c.recurring_master_id,
        q.status, q.scheduled_time, q.attempts,
        q.picked_by, q.picked_at, q.paused_from_status, q.last_error, q.version, q.updated_at,
        q.signal_key, q.signal_timeout, q.signal_payload, q.signal_payload_type,
        q.signal_outcome, q.signal_rejection_reason, q.signal_delivered_at,
        q.signal_delivered_by, q.signal_delivery_id, c.encrypted_payload\
        """;
  }

  static JobEntity hydrate(Object[] row) {
    return SHARED.hydrateRow(row);
  }

  static List<JobEntity> hydrateRows(List<Object[]> rows) {
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(hydrate(row));
    }
    return jobs;
  }

  static boolean isLiveStatus(JobStatus s) {
    return StatusClassifier.isLiveStatus(s);
  }

  static boolean isTerminalStatus(JobStatus s) {
    return StatusClassifier.isTerminalStatus(s);
  }

  static String stringOrNull(Object value) {
    return RowValues.stringOrNull(value);
  }

  static Long longOrNull(Object value) {
    return RowValues.longOrNull(value);
  }

  static UUID uuidOrNull(Object value) {
    return RowValues.uuidOrNull(value);
  }

  static JobPriority safeJobPriority(int ordinal) {
    return RowValues.safeJobPriority(ordinal);
  }

  static String payloadToJson(JobEntity job, boolean active) {
    return PayloadEncryptor.encryptArgs(
        JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(job.getPayload()),
        active,
        EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, job.getId()));
  }

  static String paramsToJson(JobEntity job, boolean active) {
    return PayloadEncryptor.encryptParamMap(
        JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getParams()),
        active,
        EncryptionTarget.rowBound(ProtectedSurface.PARAM_VALUE, job.getId()));
  }

  static String callbackPayloadToJson(
      JobEntity job, JobPayload payload, ProtectedSurface surface, boolean active) {
    return PayloadEncryptor.encryptArgs(
        JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(payload),
        active,
        EncryptionTarget.rowBound(surface, job.getId()));
  }

  static String traceContextToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getTraceContext());
  }
}
