package run.ratchet.store.mysql;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
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

final class MysqlJobRowMapper {

  // language=MySQL
  static final String HYDRATION_SELECT =
      """
      c.job_id, c.job_type, c.priority, c.max_retries, c.backoff_policy, c.backoff_param_ms,
      c.timeout_sec, c.cron_expr, c.zone_id, c.next_fire, c.payload, c.params,
      c.target_class, c.method_name, c.idempotency_key, c.business_key, c.resource_name,
      c.on_success_payload, c.on_failure_payload, c.depends_on, c.superseded_by,
      c.created_at, c.caller_principal, c.terminal_status, c.terminal_error,
      c.total_attempts, c.terminated_at, c.execution_start_time, c.execution_end_time,
      c.execution_duration_ms, c.queue_wait_ms, c.job_result, c.result_type, c.rec_status,
      c.trace_context, q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at,
      q.paused_from_status, q.last_error, q.version, q.updated_at,
      q.signal_key, q.signal_timeout, q.signal_payload, q.signal_payload_type,
      q.signal_outcome, q.signal_rejection_reason, q.signal_delivered_at,
      q.signal_delivered_by, q.signal_delivery_id\
      """;
  static final int HYDRATION_COL_COUNT = 53;
  static final int IDX_Q_STATUS = 35;
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
  private static final int IDX_CALLER_PRINCIPAL = 22;
  private static final int IDX_TERMINAL_STATUS = 23;
  private static final int IDX_TERMINAL_ERROR = 24;
  private static final int IDX_TOTAL_ATTEMPTS = 25;
  private static final int IDX_TERMINATED_AT = 26;
  private static final int IDX_EXEC_START = 27;
  private static final int IDX_EXEC_END = 28;
  private static final int IDX_EXEC_DURATION = 29;
  private static final int IDX_QUEUE_WAIT = 30;
  private static final int IDX_JOB_RESULT = 31;
  private static final int IDX_RESULT_TYPE = 32;
  private static final int IDX_REC_STATUS = 33;
  private static final int IDX_TRACE_CONTEXT = 34;
  private static final int IDX_Q_SCHEDULED_TIME = 36;
  private static final int IDX_Q_ATTEMPTS = 37;
  private static final int IDX_Q_PICKED_BY = 38;
  private static final int IDX_Q_PICKED_AT = 39;
  private static final int IDX_Q_PAUSED = 40;
  private static final int IDX_Q_LAST_ERROR = 41;
  private static final int IDX_Q_VERSION = 42;
  private static final int IDX_Q_UPDATED_AT = 43;
  private static final int IDX_Q_SIGNAL_KEY = 44;
  private static final int IDX_Q_SIGNAL_TIMEOUT = 45;
  private static final int IDX_Q_SIGNAL_PAYLOAD = 46;
  private static final int IDX_Q_SIGNAL_PAYLOAD_TYPE = 47;
  private static final int IDX_Q_SIGNAL_OUTCOME = 48;
  private static final int IDX_Q_SIGNAL_REJECTION_REASON = 49;
  private static final int IDX_Q_SIGNAL_DELIVERED_AT = 50;
  private static final int IDX_Q_SIGNAL_DELIVERED_BY = 51;
  private static final int IDX_Q_SIGNAL_DELIVERY_ID = 52;

  static boolean isTerminalStatus(JobStatus s) {
    return s == JobStatus.SUCCEEDED || s == JobStatus.FAILED || s == JobStatus.CANCELED;
  }

  static boolean isLiveStatus(JobStatus s) {
    return s == JobStatus.PENDING
        || s == JobStatus.RUNNING
        || s == JobStatus.PAUSED
        || s == JobStatus.WAITING;
  }

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return jobType == JobExecutionType.SINGLE
        || jobType == JobExecutionType.BATCH_CHILD
        || jobType == JobExecutionType.CHAIN_STEP
        || jobType == JobExecutionType.WORKFLOW_BRANCH;
  }

  static JobPriority safeJobPriority(int ordinal) {
    JobPriority[] values = JobPriority.values();
    if (ordinal < 0 || ordinal >= values.length) {
      return JobPriority.NORMAL;
    }
    return values[ordinal];
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

  static String stringOrNull(Object val) {
    if (val == null) return null;
    if (val instanceof String s) return s;
    return val.toString();
  }

  static Long longOrNull(Object val) {
    if (val == null) return null;
    if (val instanceof Number n) return n.longValue();
    return null;
  }

  static UUID uuidOrNull(Object val) {
    if (val == null) return null;
    if (val instanceof UUID uuid) return uuid;
    if (val instanceof byte[] bytes) return uuidFromBytes(bytes);
    return UUID.fromString(val.toString());
  }

  private static UUID uuidFromBytes(byte[] bytes) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException("UUID byte array must be 16 bytes, got " + bytes.length);
    }
    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (bytes[i] & 0xff);
    }
    for (int i = 8; i < 16; i++) {
      lsb = (lsb << 8) | (bytes[i] & 0xff);
    }
    return new UUID(msb, lsb);
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
      return ldt.atZone(ZoneId.systemDefault()).toInstant();
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
    j.setDependsOn(uuidOrNull(row[IDX_DEPENDS_ON]));
    j.setSupersededBy(uuidOrNull(row[IDX_SUPERSEDED_BY]));
    j.setCreatedAt(toInstant(row[IDX_CREATED_AT]));
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
          "Job %s has no live, recurring, or terminal status — possible invariant violation",
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
      Instant queueUpdatedAt = toInstant(row[IDX_Q_UPDATED_AT]);
      if (queueUpdatedAt != null) {
        j.setUpdatedAt(queueUpdatedAt);
      }
    } else if (recStatus != null) {
      j.setScheduledTime(toInstant(row[IDX_NEXT_FIRE]));
      j.setAttempts(0);
      j.setVersion(0);
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

  String payloadToJson(JobEntity job) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(job.getPayload());
  }

  String paramsToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getParams());
  }

  String callbackPayloadToJson(JobPayload payload) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(payload);
  }

  String traceContextToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getTraceContext());
  }
}
