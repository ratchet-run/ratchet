package run.ratchet.store.postgresql;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Hydrates {@link JobEntity} from the post-V005 split schema:
 *
 * <ul>
 *   <li>Cold metadata + terminal fields from {@code scheduler_job} (alias {@code c}).
 *   <li>Live state from {@code scheduler_job_queue} (alias {@code q}) when present.
 * </ul>
 *
 * <p>Status priority on hydration: {@code q.status} (live) → {@code c.rec_status} (recurring shim)
 * → {@code c.terminal_status} (terminal).
 */
final class PostgresqlJobRowMapper {

  private static final Logger log = Logger.getLogger(PostgresqlJobRowMapper.class);

  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();

  static final int HYDRATION_COL_COUNT = 44;
  static final int IDX_Q_STATUS = 35;

  private static final int IDX_JOB_ID = 0;
  private static final int IDX_JOB_TYPE = 1;
  private static final int IDX_PRIORITY = 2;
  private static final int IDX_MAX_RETRIES = 3;
  private static final int IDX_BACKOFF_POLICY = 4;
  private static final int IDX_BACKOFF_PARAM_MS = 5;
  private static final int IDX_TIMEOUT_SEC = 6;
  private static final int IDX_CRON_EXPR = 7;
  private static final int IDX_ZONE_ID = 8;
  private static final int IDX_NEXT_FIRE = 9;
  private static final int IDX_PAYLOAD = 10;
  private static final int IDX_PARAMS = 11;
  private static final int IDX_TARGET_CLASS = 12;
  private static final int IDX_METHOD_NAME = 13;
  private static final int IDX_IDEMPOTENCY_KEY = 14;
  private static final int IDX_BUSINESS_KEY = 15;
  private static final int IDX_RESOURCE_NAME = 16;
  private static final int IDX_ON_SUCCESS = 17;
  private static final int IDX_ON_FAILURE = 18;
  private static final int IDX_DEPENDS_ON = 19;
  private static final int IDX_SUPERSEDED_BY = 20;
  private static final int IDX_CREATED_AT = 21;
  private static final int IDX_CREATED_BY = 22;
  private static final int IDX_CALLER_PRINCIPAL = 23;
  private static final int IDX_TERMINAL_STATUS = 24;
  private static final int IDX_TERMINAL_ERROR = 25;
  private static final int IDX_TOTAL_ATTEMPTS = 26;
  private static final int IDX_TERMINATED_AT = 27;
  private static final int IDX_EXEC_START = 28;
  private static final int IDX_EXEC_END = 29;
  private static final int IDX_EXEC_DURATION = 30;
  private static final int IDX_QUEUE_WAIT = 31;
  private static final int IDX_JOB_RESULT = 32;
  private static final int IDX_RESULT_TYPE = 33;
  private static final int IDX_REC_STATUS = 34;
  private static final int IDX_Q_SCHEDULED_TIME = 36;
  private static final int IDX_Q_ATTEMPTS = 37;
  private static final int IDX_Q_PICKED_BY = 38;
  private static final int IDX_Q_PICKED_AT = 39;
  private static final int IDX_Q_PAUSED = 40;
  private static final int IDX_Q_LAST_ERROR = 41;
  private static final int IDX_Q_VERSION = 42;
  private static final int IDX_Q_UPDATED_AT = 43;

  private PostgresqlJobRowMapper() {}

  /**
   * Returns a SELECT projection joining the cold metadata table {@code scheduler_job} (alias {@code
   * c}) with the hot queue table {@code scheduler_job_queue} (alias {@code q}). Callers must
   * include {@code FROM scheduler_job c LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id} in
   * their query. The column order matches {@link #hydrate(Object[])}.
   */
  static String hydrationSelect() {
    return "c.job_id, c.job_type, c.priority, c.max_retries, c.backoff_policy, "
        + "c.backoff_param_ms, c.timeout_sec, c.cron_expr, c.zone_id, c.next_fire, "
        + "c.payload::text, c.params::text, c.target_class, c.method_name, c.idempotency_key, "
        + "c.business_key, c.resource_name, c.on_success_payload::text, "
        + "c.on_failure_payload::text, c.depends_on, c.superseded_by, c.created_at, "
        + "c.created_by, c.caller_principal, c.terminal_status, c.terminal_error, "
        + "c.total_attempts, c.terminated_at, c.execution_start_time, c.execution_end_time, "
        + "c.execution_duration_ms, c.queue_wait_ms, c.job_result::text, c.result_type, "
        + "c.rec_status, q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
        + "q.paused_from_status, q.last_error, q.version, q.updated_at";
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
    JobEntity j = new JobEntity();
    j.setId(((Number) row[IDX_JOB_ID]).longValue());
    j.setJobType(JobExecutionType.valueOf((String) row[IDX_JOB_TYPE]));
    j.setPriority(safeJobPriority(((Number) row[IDX_PRIORITY]).intValue()));
    j.setMaxRetries(((Number) row[IDX_MAX_RETRIES]).intValue());
    j.setBackoffPolicy(BackoffPolicy.valueOf((String) row[IDX_BACKOFF_POLICY]));
    j.setBackoffParamMs(((Number) row[IDX_BACKOFF_PARAM_MS]).intValue());
    j.setTimeoutSec(((Number) row[IDX_TIMEOUT_SEC]).intValue());
    j.setCronExpr((String) row[IDX_CRON_EXPR]);
    j.setZoneId((String) row[IDX_ZONE_ID]);
    j.setNextFire(toInstant(row[IDX_NEXT_FIRE]));
    j.setPayload(JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_PAYLOAD])));
    j.setParams(JSON_MAP_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_PARAMS])));
    j.setTargetClass((String) row[IDX_TARGET_CLASS]);
    j.setMethodName((String) row[IDX_METHOD_NAME]);
    j.setIdempotencyKey((String) row[IDX_IDEMPOTENCY_KEY]);
    j.setBusinessKey((String) row[IDX_BUSINESS_KEY]);
    j.setResourceName((String) row[IDX_RESOURCE_NAME]);
    j.setOnSuccessPayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_ON_SUCCESS])));
    j.setOnFailurePayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_ON_FAILURE])));
    j.setDependsOn(longOrNull(row[IDX_DEPENDS_ON]));
    j.setSupersededBy(longOrNull(row[IDX_SUPERSEDED_BY]));
    j.setCreatedAt(toInstant(row[IDX_CREATED_AT]));
    j.setCreatedBy((String) row[IDX_CREATED_BY]);
    j.setCallerPrincipal((String) row[IDX_CALLER_PRINCIPAL]);

    String terminalStr = (String) row[IDX_TERMINAL_STATUS];
    JobStatus terminal = terminalStr != null ? JobStatus.valueOf(terminalStr) : null;
    j.setTerminalStatus(terminal);

    j.setExecutionStartTime(toInstant(row[IDX_EXEC_START]));
    j.setExecutionEndTime(toInstant(row[IDX_EXEC_END]));
    j.setExecutionDurationMs(longOrNull(row[IDX_EXEC_DURATION]));
    j.setQueueWaitMs(longOrNull(row[IDX_QUEUE_WAIT]));
    j.setJobResult(stringOrNull(row[IDX_JOB_RESULT]));
    j.setResultType((String) row[IDX_RESULT_TYPE]);

    String recStatus = stringOrNull(row[IDX_REC_STATUS]);
    String liveStr = (String) row[IDX_Q_STATUS];
    JobStatus live = liveStr != null ? JobStatus.valueOf(liveStr) : null;

    JobStatus resolved;
    if (live != null) {
      resolved = live;
    } else if (recStatus != null) {
      resolved = recStatusDecode(recStatus);
    } else if (terminal != null) {
      resolved = terminal;
    } else {
      log.errorf(
          "Job %d has no live, recurring, or terminal status — possible invariant violation",
          j.getId());
      resolved = null;
    }
    j.setStatus(resolved);

    if (live != null) {
      j.setScheduledTime(toInstant(row[IDX_Q_SCHEDULED_TIME]));
      j.setAttempts(((Number) row[IDX_Q_ATTEMPTS]).intValue());
      j.setPickedBy((String) row[IDX_Q_PICKED_BY]);
      j.setPickedAt(toInstant(row[IDX_Q_PICKED_AT]));
      String pausedFrom = (String) row[IDX_Q_PAUSED];
      j.setPausedFromStatus(pausedFrom != null ? JobStatus.valueOf(pausedFrom) : null);
      j.setLastError(stringOrNull(row[IDX_Q_LAST_ERROR]));
      j.setVersion(((Number) row[IDX_Q_VERSION]).intValue());
      Instant updatedAt = toInstant(row[IDX_Q_UPDATED_AT]);
      j.setUpdatedAt(updatedAt != null ? updatedAt : j.getCreatedAt());
    } else if (recStatus != null) {
      j.setScheduledTime(toInstant(row[IDX_NEXT_FIRE]));
      j.setAttempts(0);
      j.setVersion(0);
      j.setUpdatedAt(j.getCreatedAt());
    } else {
      Number ta = (Number) row[IDX_TOTAL_ATTEMPTS];
      j.setAttempts(ta != null ? ta.intValue() : 0);
      j.setLastError(stringOrNull(row[IDX_TERMINAL_ERROR]));
      j.setVersion(0);
      Instant fallbackSched = toInstant(row[IDX_EXEC_START]);
      if (fallbackSched == null) {
        fallbackSched = toInstant(row[IDX_CREATED_AT]);
      }
      j.setScheduledTime(fallbackSched);
      Instant updatedAt = toInstant(row[IDX_TERMINATED_AT]);
      j.setUpdatedAt(updatedAt != null ? updatedAt : j.getCreatedAt());
    }
    return j;
  }

  static List<JobEntity> hydrateRows(List<Object[]> rows) {
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(hydrate(row));
    }
    return jobs;
  }

  static String recStatusForLiveStatus(JobStatus s) {
    if (s == JobStatus.PENDING) return "P";
    if (s == JobStatus.PAUSED) return "A";
    return null;
  }

  static JobStatus recStatusDecode(String c) {
    if ("P".equals(c)) return JobStatus.PENDING;
    if ("A".equals(c)) return JobStatus.PAUSED;
    return null;
  }

  static boolean isLiveStatus(JobStatus s) {
    return s == JobStatus.PENDING || s == JobStatus.RUNNING || s == JobStatus.PAUSED;
  }

  static boolean isTerminalStatus(JobStatus s) {
    return s == JobStatus.SUCCEEDED || s == JobStatus.FAILED || s == JobStatus.CANCELED;
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

  static String payloadToJson(JobEntity job) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(job.getPayload());
  }

  static String paramsToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getParams());
  }

  static String callbackPayloadToJson(JobPayload payload) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(payload);
  }
}
