package run.ratchet.store.mysql;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.util.RowValues;
import run.ratchet.store.util.StatusClassifier;

final class MysqlJobRowMapper {

  // language=MySQL
  static final String HYDRATION_SELECT =
      """
      c.job_id, c.job_type, c.priority, c.max_retries, c.backoff_policy, c.backoff_param_ms,
      c.timeout_sec, c.cron_expr, c.zone_id, c.payload, c.params,
      c.target_class, c.method_name, c.idempotency_key, c.business_key, c.resource_name,
      c.on_success_payload, c.on_failure_payload, c.depends_on, c.superseded_by,
      c.created_at, c.caller_principal, c.terminal_status, c.terminal_error,
      c.total_attempts, c.terminated_at, c.execution_start_time, c.execution_end_time,
      c.execution_duration_ms, c.queue_wait_ms, c.job_result, c.result_type,
      c.trace_context, q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at,
      q.paused_from_status, q.last_error, q.version, q.updated_at,
      q.signal_key, q.signal_timeout, q.signal_payload, q.signal_payload_type,
      q.signal_outcome, q.signal_rejection_reason, q.signal_delivered_at,
      q.signal_delivered_by, q.signal_delivery_id\
      """;
  static final int HYDRATION_COL_COUNT = 51;
  static final int IDX_Q_STATUS = 33;
  private static final Logger log = Logger.getLogger(MysqlJobRowMapper.class);
  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();
  private static final int IDX_JOB_ID = 0;
  private static final int IDX_JOB_TYPE = 1;
  private static final int IDX_PRIORITY = 2;
  private static final int IDX_MAX_RETRIES = 3;
  private static final int IDX_BACKOFF_POLICY = 4;
  private static final int IDX_BACKOFF_PARAM_MS = 5;
  private static final int IDX_TIMEOUT_SEC = 6;
  private static final int IDX_CRON_EXPR = 7;
  private static final int IDX_ZONE_ID = 8;
  private static final int IDX_PAYLOAD = 9;
  private static final int IDX_PARAMS = 10;
  private static final int IDX_TARGET_CLASS = 11;
  private static final int IDX_METHOD_NAME = 12;
  private static final int IDX_IDEMPOTENCY_KEY = 13;
  private static final int IDX_BUSINESS_KEY = 14;
  private static final int IDX_RESOURCE_NAME = 15;
  private static final int IDX_ON_SUCCESS = 16;
  private static final int IDX_ON_FAILURE = 17;
  private static final int IDX_DEPENDS_ON = 18;
  private static final int IDX_SUPERSEDED_BY = 19;
  private static final int IDX_CREATED_AT = 20;
  private static final int IDX_CALLER_PRINCIPAL = 21;
  private static final int IDX_TERMINAL_STATUS = 22;
  private static final int IDX_TERMINAL_ERROR = 23;
  private static final int IDX_TOTAL_ATTEMPTS = 24;
  private static final int IDX_TERMINATED_AT = 25;
  private static final int IDX_EXEC_START = 26;
  private static final int IDX_EXEC_END = 27;
  private static final int IDX_EXEC_DURATION = 28;
  private static final int IDX_QUEUE_WAIT = 29;
  private static final int IDX_JOB_RESULT = 30;
  private static final int IDX_RESULT_TYPE = 31;
  private static final int IDX_TRACE_CONTEXT = 32;
  private static final int IDX_Q_SCHEDULED_TIME = 34;
  private static final int IDX_Q_ATTEMPTS = 35;
  private static final int IDX_Q_PICKED_BY = 36;
  private static final int IDX_Q_PICKED_AT = 37;
  private static final int IDX_Q_PAUSED = 38;
  private static final int IDX_Q_LAST_ERROR = 39;
  private static final int IDX_Q_VERSION = 40;
  private static final int IDX_Q_UPDATED_AT = 41;
  private static final int IDX_Q_SIGNAL_KEY = 42;
  private static final int IDX_Q_SIGNAL_TIMEOUT = 43;
  private static final int IDX_Q_SIGNAL_PAYLOAD = 44;
  private static final int IDX_Q_SIGNAL_PAYLOAD_TYPE = 45;
  private static final int IDX_Q_SIGNAL_OUTCOME = 46;
  private static final int IDX_Q_SIGNAL_REJECTION_REASON = 47;
  private static final int IDX_Q_SIGNAL_DELIVERED_AT = 48;
  private static final int IDX_Q_SIGNAL_DELIVERED_BY = 49;
  private static final int IDX_Q_SIGNAL_DELIVERY_ID = 50;

  static boolean isTerminalStatus(JobStatus s) {
    return StatusClassifier.isTerminalStatus(s);
  }

  static boolean isLiveStatus(JobStatus s) {
    return StatusClassifier.isLiveStatus(s);
  }

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return StatusClassifier.isPollerExecutable(jobType);
  }

  static JobPriority safeJobPriority(int ordinal) {
    return RowValues.safeJobPriority(ordinal);
  }

  static String stringOrNull(Object val) {
    return RowValues.stringOrNull(val);
  }

  static Long longOrNull(Object val) {
    return RowValues.longOrNull(val);
  }

  static UUID uuidOrNull(Object val) {
    return RowValues.uuidOrNull(val);
  }

  static Instant toInstant(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Timestamp ts) {
      return ts.toInstant();
    }
    if (val instanceof Instant inst) {
      return inst;
    }
    if (val instanceof LocalDateTime ldt) {
      return ldt.toInstant(ZoneOffset.UTC);
    }
    if (val instanceof Date date) {
      return date.toInstant();
    }
    return null;
  }

  JobEntity hydrateJobEntity(Object[] row) {
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
    j.setId(uuidOrNull(row[IDX_JOB_ID]));
    j.setJobType(enumValue(row, IDX_JOB_TYPE, "job_type", JobExecutionType.class));
    j.setPriority(safeJobPriority(requiredNumber(row, IDX_PRIORITY, "priority").intValue()));
    j.setMaxRetries(requiredNumber(row, IDX_MAX_RETRIES, "max_retries").intValue());
    j.setBackoffPolicy(enumValue(row, IDX_BACKOFF_POLICY, "backoff_policy", BackoffPolicy.class));
    j.setBackoffParamMs(requiredNumber(row, IDX_BACKOFF_PARAM_MS, "backoff_param_ms").intValue());
    j.setTimeoutSec(requiredNumber(row, IDX_TIMEOUT_SEC, "timeout_sec").intValue());
    j.setCronExpr((String) row[IDX_CRON_EXPR]);
    j.setZoneId((String) row[IDX_ZONE_ID]);
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
    j.setDependsOn(uuidOrNull(row[IDX_DEPENDS_ON]));
    j.setSupersededBy(uuidOrNull(row[IDX_SUPERSEDED_BY]));
    j.setCreatedAt(toInstant(row[IDX_CREATED_AT]));
    j.setCallerPrincipal((String) row[IDX_CALLER_PRINCIPAL]);

    JobStatus terminal =
        enumValueOrNull(row, IDX_TERMINAL_STATUS, "terminal_status", JobStatus.class);
    j.setTerminalStatus(terminal);

    j.setExecutionStartTime(toInstant(row[IDX_EXEC_START]));
    j.setExecutionEndTime(toInstant(row[IDX_EXEC_END]));
    j.setExecutionDurationMs(longOrNull(row[IDX_EXEC_DURATION]));
    j.setQueueWaitMs(longOrNull(row[IDX_QUEUE_WAIT]));
    j.setJobResult(stringOrNull(row[IDX_JOB_RESULT]));
    j.setResultType((String) row[IDX_RESULT_TYPE]);
    j.setTraceContext(
        JSON_MAP_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_TRACE_CONTEXT])));
    j.setSignalKey((String) row[IDX_Q_SIGNAL_KEY]);
    j.setSignalTimeout(toInstant(row[IDX_Q_SIGNAL_TIMEOUT]));
    j.setSignalPayload(stringOrNull(row[IDX_Q_SIGNAL_PAYLOAD]));
    j.setSignalPayloadType(stringOrNull(row[IDX_Q_SIGNAL_PAYLOAD_TYPE]));
    j.setSignalOutcome(stringOrNull(row[IDX_Q_SIGNAL_OUTCOME]));
    j.setSignalRejectionReason(stringOrNull(row[IDX_Q_SIGNAL_REJECTION_REASON]));
    j.setSignalDeliveredAt(toInstant(row[IDX_Q_SIGNAL_DELIVERED_AT]));
    j.setSignalDeliveredBy((String) row[IDX_Q_SIGNAL_DELIVERED_BY]);
    j.setSignalDeliveryId(stringOrNull(row[IDX_Q_SIGNAL_DELIVERY_ID]));

    JobStatus live = enumValueOrNull(row, IDX_Q_STATUS, "q.status", JobStatus.class);

    JobStatus resolved;
    if (live != null) {
      resolved = live;
    } else if (terminal != null) {
      resolved = terminal;
    } else {
      log.errorf("Job %s has no live or terminal status — possible invariant violation", j.getId());
      resolved = null;
    }
    j.setStatus(resolved);

    if (live != null) {
      j.setScheduledTime(toInstant(row[IDX_Q_SCHEDULED_TIME]));
      j.setAttempts(requiredNumber(row, IDX_Q_ATTEMPTS, "q.attempts").intValue());
      j.setPickedBy((String) row[IDX_Q_PICKED_BY]);
      j.setPickedAt(toInstant(row[IDX_Q_PICKED_AT]));
      j.setPausedFromStatus(
          enumValueOrNull(row, IDX_Q_PAUSED, "q.paused_from_status", JobStatus.class));
      j.setLastError(stringOrNull(row[IDX_Q_LAST_ERROR]));
      j.setVersion(requiredNumber(row, IDX_Q_VERSION, "q.version").intValue());
      Instant queueUpdatedAt = toInstant(row[IDX_Q_UPDATED_AT]);
      if (queueUpdatedAt != null) {
        j.setUpdatedAt(queueUpdatedAt);
      }
    } else {
      Number ta = numberOrNull(row, IDX_TOTAL_ATTEMPTS, "total_attempts");
      j.setAttempts(ta != null ? ta.intValue() : 0);
      j.setLastError(stringOrNull(row[IDX_TERMINAL_ERROR]));
      j.setVersion(0);
      Instant fallbackSched = toInstant(row[IDX_EXEC_START]);
      if (fallbackSched == null) {
        fallbackSched = toInstant(row[IDX_CREATED_AT]);
      }
      j.setScheduledTime(fallbackSched);
    }

    if (j.getUpdatedAt() == null) {
      Instant updatedAt = toInstant(row[IDX_TERMINATED_AT]);
      if (updatedAt == null) {
        updatedAt = j.getCreatedAt();
      }
      j.setUpdatedAt(updatedAt);
    }

    return j;
  }

  static List<JobEntity> hydrateRows(List<Object[]> rows) {
    MysqlJobRowMapper mapper = new MysqlJobRowMapper();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    return jobs;
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

  static String traceContextToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getTraceContext());
  }

  private static <E extends Enum<E>> E enumValue(
      Object[] row, int index, String column, Class<E> enumType) {
    String raw = stringOrNull(row[index]);
    if (raw == null) {
      throw hydrationFailure(row, index, column, null, null);
    }
    try {
      return Enum.valueOf(enumType, raw);
    } catch (IllegalArgumentException e) {
      throw hydrationFailure(row, index, column, raw, e);
    }
  }

  private static <E extends Enum<E>> E enumValueOrNull(
      Object[] row, int index, String column, Class<E> enumType) {
    String raw = stringOrNull(row[index]);
    if (raw == null) {
      return null;
    }
    try {
      return Enum.valueOf(enumType, raw);
    } catch (IllegalArgumentException e) {
      throw hydrationFailure(row, index, column, raw, e);
    }
  }

  private static Number requiredNumber(Object[] row, int index, String column) {
    Number number = numberOrNull(row, index, column);
    if (number == null) {
      throw hydrationFailure(row, index, column, null, null);
    }
    return number;
  }

  private static Number numberOrNull(Object[] row, int index, String column) {
    Object value = row[index];
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number;
    }
    throw hydrationFailure(row, index, column, value, null);
  }

  private static JobHydrationException hydrationFailure(
      Object[] row, int index, String column, Object value, Throwable cause) {
    return new JobHydrationException(
        "Failed to hydrate MySQL job "
            + safeJobId(row)
            + ": column "
            + column
            + " at index "
            + index
            + " has value "
            + value,
        cause);
  }

  private static UUID safeJobId(Object[] row) {
    try {
      return uuidOrNull(row[IDX_JOB_ID]);
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  static final class JobHydrationException extends IllegalStateException {
    JobHydrationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
