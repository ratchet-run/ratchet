package run.ratchet.store.postgresql;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

final class PostgresqlJobRowMapper {

  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();

  private static final int HYDRATION_COL_COUNT = 39;
  private static final int IDX_JOB_ID = 0;
  private static final int IDX_STATUS = 1;
  private static final int IDX_PAUSED_FROM_STATUS = 2;
  private static final int IDX_SCHEDULED_TIME = 3;
  private static final int IDX_JOB_TYPE = 4;
  private static final int IDX_PRIORITY = 5;
  private static final int IDX_ATTEMPTS = 6;
  private static final int IDX_MAX_RETRIES = 7;
  private static final int IDX_BACKOFF_POLICY = 8;
  private static final int IDX_BACKOFF_PARAM_MS = 9;
  private static final int IDX_TIMEOUT_SEC = 10;
  private static final int IDX_CRON_EXPR = 11;
  private static final int IDX_ZONE_ID = 12;
  private static final int IDX_NEXT_FIRE = 13;
  private static final int IDX_PAYLOAD = 14;
  private static final int IDX_PARAMS = 15;
  private static final int IDX_TARGET_CLASS = 16;
  private static final int IDX_METHOD_NAME = 17;
  private static final int IDX_IDEMPOTENCY_KEY = 18;
  private static final int IDX_BUSINESS_KEY = 19;
  private static final int IDX_RESOURCE_NAME = 20;
  private static final int IDX_ON_SUCCESS = 21;
  private static final int IDX_ON_FAILURE = 22;
  private static final int IDX_DEPENDS_ON = 23;
  private static final int IDX_SUPERSEDED_BY = 24;
  private static final int IDX_PICKED_BY = 25;
  private static final int IDX_PICKED_AT = 26;
  private static final int IDX_LAST_ERROR = 27;
  private static final int IDX_CREATED_AT = 28;
  private static final int IDX_CREATED_BY = 29;
  private static final int IDX_CALLER_PRINCIPAL = 30;
  private static final int IDX_UPDATED_AT = 31;
  private static final int IDX_EXEC_START = 32;
  private static final int IDX_EXEC_END = 33;
  private static final int IDX_EXEC_DURATION = 34;
  private static final int IDX_QUEUE_WAIT = 35;
  private static final int IDX_JOB_RESULT = 36;
  private static final int IDX_RESULT_TYPE = 37;
  private static final int IDX_VERSION = 38;

  private PostgresqlJobRowMapper() {}

  static String hydrationSelect(String alias) {
    String prefix = alias == null || alias.isBlank() ? "" : alias + ".";
    return prefix
        + "job_id, "
        + prefix
        + "status, "
        + prefix
        + "paused_from_status, "
        + prefix
        + "scheduled_time, "
        + prefix
        + "job_type, "
        + prefix
        + "priority, "
        + prefix
        + "attempts, "
        + prefix
        + "max_retries, "
        + prefix
        + "backoff_policy, "
        + prefix
        + "backoff_param_ms, "
        + prefix
        + "timeout_sec, "
        + prefix
        + "cron_expr, "
        + prefix
        + "zone_id, "
        + prefix
        + "next_fire, "
        + prefix
        + "payload::text, "
        + prefix
        + "params::text, "
        + prefix
        + "target_class, "
        + prefix
        + "method_name, "
        + prefix
        + "idempotency_key, "
        + prefix
        + "business_key, "
        + prefix
        + "resource_name, "
        + prefix
        + "on_success_payload::text, "
        + prefix
        + "on_failure_payload::text, "
        + prefix
        + "depends_on, "
        + prefix
        + "superseded_by, "
        + prefix
        + "picked_by, "
        + prefix
        + "picked_at, "
        + prefix
        + "last_error, "
        + prefix
        + "created_at, "
        + prefix
        + "created_by, "
        + prefix
        + "caller_principal, "
        + prefix
        + "updated_at, "
        + prefix
        + "execution_start_time, "
        + prefix
        + "execution_end_time, "
        + prefix
        + "execution_duration_ms, "
        + prefix
        + "queue_wait_ms, "
        + prefix
        + "job_result::text, "
        + prefix
        + "result_type, "
        + prefix
        + "version";
  }

  static JobEntity hydrate(Object[] row) {
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
    JobEntity job = new JobEntity();
    job.setId(((Number) row[IDX_JOB_ID]).longValue());
    JobStatus status = JobStatus.valueOf((String) row[IDX_STATUS]);
    job.setStatus(status);
    if (status.isTerminal()) {
      job.setTerminalStatus(status);
    }
    String pausedFrom = (String) row[IDX_PAUSED_FROM_STATUS];
    job.setPausedFromStatus(pausedFrom == null ? null : JobStatus.valueOf(pausedFrom));
    job.setScheduledTime(toInstant(row[IDX_SCHEDULED_TIME]));
    job.setJobType(JobExecutionType.valueOf((String) row[IDX_JOB_TYPE]));
    job.setPriority(safeJobPriority(((Number) row[IDX_PRIORITY]).intValue()));
    job.setAttempts(((Number) row[IDX_ATTEMPTS]).intValue());
    job.setMaxRetries(((Number) row[IDX_MAX_RETRIES]).intValue());
    job.setBackoffPolicy(BackoffPolicy.valueOf((String) row[IDX_BACKOFF_POLICY]));
    job.setBackoffParamMs(((Number) row[IDX_BACKOFF_PARAM_MS]).intValue());
    job.setTimeoutSec(((Number) row[IDX_TIMEOUT_SEC]).intValue());
    job.setCronExpr((String) row[IDX_CRON_EXPR]);
    job.setZoneId((String) row[IDX_ZONE_ID]);
    job.setNextFire(toInstant(row[IDX_NEXT_FIRE]));
    job.setPayload(JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_PAYLOAD])));
    job.setParams(JSON_MAP_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_PARAMS])));
    job.setTargetClass((String) row[IDX_TARGET_CLASS]);
    job.setMethodName((String) row[IDX_METHOD_NAME]);
    job.setIdempotencyKey((String) row[IDX_IDEMPOTENCY_KEY]);
    job.setBusinessKey((String) row[IDX_BUSINESS_KEY]);
    job.setResourceName((String) row[IDX_RESOURCE_NAME]);
    job.setOnSuccessPayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_ON_SUCCESS])));
    job.setOnFailurePayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_ON_FAILURE])));
    job.setDependsOn(longOrNull(row[IDX_DEPENDS_ON]));
    job.setSupersededBy(longOrNull(row[IDX_SUPERSEDED_BY]));
    job.setPickedBy((String) row[IDX_PICKED_BY]);
    job.setPickedAt(toInstant(row[IDX_PICKED_AT]));
    job.setLastError(stringOrNull(row[IDX_LAST_ERROR]));
    job.setCreatedAt(toInstant(row[IDX_CREATED_AT]));
    job.setCreatedBy((String) row[IDX_CREATED_BY]);
    job.setCallerPrincipal((String) row[IDX_CALLER_PRINCIPAL]);
    job.setUpdatedAt(toInstant(row[IDX_UPDATED_AT]));
    job.setExecutionStartTime(toInstant(row[IDX_EXEC_START]));
    job.setExecutionEndTime(toInstant(row[IDX_EXEC_END]));
    job.setExecutionDurationMs(longOrNull(row[IDX_EXEC_DURATION]));
    job.setQueueWaitMs(longOrNull(row[IDX_QUEUE_WAIT]));
    job.setJobResult(stringOrNull(row[IDX_JOB_RESULT]));
    job.setResultType((String) row[IDX_RESULT_TYPE]);
    job.setVersion(row[IDX_VERSION] == null ? null : ((Number) row[IDX_VERSION]).intValue());
    return job;
  }

  static List<JobEntity> hydrateRows(List<Object[]> rows) {
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(hydrate(row));
    }
    return jobs;
  }

  static Instant toInstant(Object value) {
    if (value instanceof Instant i) {
      return i;
    }
    if (value instanceof Timestamp t) {
      return t.toInstant();
    }
    if (value instanceof OffsetDateTime odt) {
      return odt.toInstant();
    }
    return null;
  }

  static String stringOrNull(Object value) {
    if (value == null) return null;
    if (value instanceof String s) return s;
    return value.toString();
  }

  static Long longOrNull(Object value) {
    if (value == null) return null;
    if (value instanceof Number n) return n.longValue();
    return null;
  }

  static JobPriority safeJobPriority(int ordinal) {
    JobPriority[] values = JobPriority.values();
    if (ordinal < 0 || ordinal >= values.length) {
      return JobPriority.NORMAL;
    }
    return values[ordinal];
  }
}
