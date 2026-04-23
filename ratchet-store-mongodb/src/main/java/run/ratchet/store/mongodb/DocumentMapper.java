package run.ratchet.store.mongodb;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;

/** Bidirectional mapping between Ratchet store-core entities and MongoDB BSON documents. */
public final class DocumentMapper {

  private DocumentMapper() {}

  public static Document toDocument(JobEntity job) {
    Document doc = new Document();
    if (job.getId() != null) {
      doc.append("_id", job.getId());
    }
    doc.append("status", job.getStatus() == null ? "PENDING" : job.getStatus().name());
    doc.append(
        "paused_from_status",
        job.getPausedFromStatus() == null ? null : job.getPausedFromStatus().name());
    doc.append("scheduled_time", toDate(job.getScheduledTime()));
    doc.append("job_type", job.getJobType().name());
    doc.append("priority", job.getPriority().ordinal());
    doc.append("attempts", job.getAttempts());
    doc.append("max_retries", job.getMaxRetries());
    doc.append("backoff_policy", job.getBackoffPolicy().name());
    doc.append("backoff_param_ms", job.getBackoffParamMs());
    doc.append("timeout_sec", job.getTimeoutSec());
    doc.append("cron_expr", job.getCronExpr() == null ? "" : job.getCronExpr());
    doc.append("zone_id", job.getZoneId() == null ? "UTC" : job.getZoneId());
    doc.append("next_fire", toDate(job.getNextFire()));
    doc.append("payload", payloadToDocument(job.getPayload()));
    doc.append("params", paramsToDocument(job.getParams()));
    doc.append(
        "target_class",
        job.getPayload() != null ? job.getPayload().target() : job.getTargetClass());
    doc.append(
        "method_name", job.getPayload() != null ? job.getPayload().method() : job.getMethodName());
    doc.append("idempotency_key", job.getIdempotencyKey());
    doc.append("business_key", job.getBusinessKey());
    doc.append("tags", job.getTags() != null ? job.getTags() : List.of());
    doc.append("resource_name", job.getResourceName());
    doc.append("on_success_payload", payloadToDocument(job.getOnSuccessPayload()));
    doc.append("on_failure_payload", payloadToDocument(job.getOnFailurePayload()));
    doc.append("depends_on", job.getDependsOn());
    doc.append("superseded_by", job.getSupersededBy());
    doc.append("picked_by", job.getPickedBy());
    doc.append("picked_at", toDate(job.getPickedAt()));
    doc.append("last_error", job.getLastError());
    doc.append("created_at", toDate(job.getCreatedAt()));
    doc.append("created_by", job.getCreatedBy());
    doc.append("caller_principal", job.getCallerPrincipal());
    doc.append("updated_at", toDate(job.getUpdatedAt()));
    doc.append("execution_start_time", toDate(job.getExecutionStartTime()));
    doc.append("execution_end_time", toDate(job.getExecutionEndTime()));
    doc.append("execution_duration_ms", job.getExecutionDurationMs());
    doc.append("queue_wait_ms", job.getQueueWaitMs());
    doc.append("job_result", job.getJobResult());
    doc.append("result_type", job.getResultType());
    doc.append("version", job.getVersion() == null ? 0 : job.getVersion());
    return doc;
  }

  public static JobEntity toJobEntity(Document doc) {
    JobEntity job = new JobEntity();
    job.setId(doc.getLong("_id"));
    job.setStatus(JobStatus.valueOf(doc.getString("status")));
    if (doc.getString("paused_from_status") != null) {
      job.setPausedFromStatus(JobStatus.valueOf(doc.getString("paused_from_status")));
    }
    job.setScheduledTime(toInstant(doc.getDate("scheduled_time")));
    job.setJobType(JobExecutionType.valueOf(doc.getString("job_type")));
    job.setPriority(safeJobPriority(doc.getInteger("priority", 2)));
    job.setAttempts(doc.getInteger("attempts", 0));
    job.setMaxRetries(doc.getInteger("max_retries", 0));
    job.setBackoffPolicy(BackoffPolicy.valueOf(doc.getString("backoff_policy")));
    job.setBackoffParamMs(doc.getInteger("backoff_param_ms", 0));
    job.setTimeoutSec(doc.getInteger("timeout_sec", 0));
    job.setCronExpr(doc.getString("cron_expr"));
    job.setZoneId(doc.getString("zone_id"));
    job.setNextFire(toInstant(doc.getDate("next_fire")));
    job.setPayload(documentToPayload(doc.get("payload", Document.class)));
    job.setParams(documentToParams(doc.get("params", Document.class)));
    job.setTargetClass(doc.getString("target_class"));
    job.setMethodName(doc.getString("method_name"));
    job.setIdempotencyKey(doc.getString("idempotency_key"));
    job.setBusinessKey(doc.getString("business_key"));
    job.setTags(doc.getList("tags", String.class));
    job.setResourceName(doc.getString("resource_name"));
    job.setOnSuccessPayload(documentToPayload(doc.get("on_success_payload", Document.class)));
    job.setOnFailurePayload(documentToPayload(doc.get("on_failure_payload", Document.class)));
    job.setDependsOn(doc.getLong("depends_on"));
    job.setSupersededBy(doc.getLong("superseded_by"));
    job.setPickedBy(doc.getString("picked_by"));
    job.setPickedAt(toInstant(doc.getDate("picked_at")));
    job.setLastError(doc.getString("last_error"));
    job.setCreatedAt(toInstant(doc.getDate("created_at")));
    job.setCreatedBy(doc.getString("created_by"));
    job.setCallerPrincipal(doc.getString("caller_principal"));
    job.setUpdatedAt(toInstant(doc.getDate("updated_at")));
    job.setExecutionStartTime(toInstant(doc.getDate("execution_start_time")));
    job.setExecutionEndTime(toInstant(doc.getDate("execution_end_time")));
    job.setExecutionDurationMs(doc.getLong("execution_duration_ms"));
    job.setQueueWaitMs(doc.getLong("queue_wait_ms"));
    job.setJobResult(doc.getString("job_result"));
    job.setResultType(doc.getString("result_type"));
    job.setVersion(doc.getInteger("version", 0));
    return job;
  }

  public static JobClaimDto toJobClaimDto(Document doc) {
    return new JobClaimDto(
        doc.getLong("_id"),
        JobStatus.valueOf(doc.getString("status")),
        JobExecutionType.valueOf(doc.getString("job_type")),
        safeJobPriority(doc.getInteger("priority", 2)),
        toInstant(doc.getDate("scheduled_time")),
        doc.getInteger("version"),
        doc.getInteger("timeout_sec", 0),
        doc.getString("picked_by"),
        toInstant(doc.getDate("picked_at")),
        doc.getString("business_key"),
        doc.getInteger("attempts", 0),
        doc.getInteger("max_retries", 0));
  }

  public static Document toDocument(BatchEntity batch) {
    Document doc = new Document();
    doc.append("_id", batch.getId());
    doc.append("total_items", batch.getTotalItems());
    doc.append("completed_items", batch.getCompletedItems());
    doc.append("failed_items", batch.getFailedItems());
    doc.append("completion_processed", batch.getCompletionProcessed());
    doc.append("version", batch.getVersion() == null ? 0 : batch.getVersion());
    doc.append("progress_hook", payloadToDocument(batch.getProgressHook()));
    return doc;
  }

  public static BatchEntity toBatchEntity(Document doc) {
    BatchEntity batch = new BatchEntity();
    batch.setId(doc.getLong("_id"));
    batch.setTotalItems(doc.getInteger("total_items", 0));
    batch.setCompletedItems(doc.getInteger("completed_items", 0));
    batch.setFailedItems(doc.getInteger("failed_items", 0));
    batch.setCompletionProcessed(doc.getBoolean("completion_processed", false));
    batch.setVersion(doc.getInteger("version", 0));
    batch.setProgressHook(documentToPayload(doc.get("progress_hook", Document.class)));
    return batch;
  }

  public static BatchProgress toBatchProgress(Document doc, long batchId) {
    return new BatchProgress(
        batchId,
        doc.getInteger("total_items", 0),
        doc.getInteger("completed_items", 0),
        doc.getInteger("failed_items", 0),
        documentToPayload(doc.get("progress_hook", Document.class)));
  }

  public static Document toDocument(NodeEntity node) {
    Document doc = new Document();
    doc.append("_id", node.getId());
    doc.append("heartbeat_ts", toDate(node.getLastHeartbeat()));
    doc.append("started_at", toDate(node.getStartedAt()));
    doc.append("node_info", nodeInfoToDocument(node.getNodeInfo()));
    return doc;
  }

  public static NodeEntity toNodeEntity(Document doc) {
    NodeEntity node = new NodeEntity();
    node.setId(doc.getString("_id"));
    node.setLastHeartbeat(toInstant(doc.getDate("heartbeat_ts")));
    node.setStartedAt(toInstant(doc.getDate("started_at")));
    node.setNodeInfo(documentToNodeInfo(doc.get("node_info", Document.class)));
    return node;
  }

  public static Document toDocument(JobExecutionEntity exec) {
    Document doc = new Document();
    if (exec.getId() != null) {
      doc.append("_id", exec.getId());
    }
    doc.append("job_id", exec.getJobId());
    doc.append("attempt", exec.getAttempt());
    doc.append("node_id", exec.getNodeId());
    doc.append("started_at", toDate(exec.getStartedAt()));
    doc.append("ended_at", toDate(exec.getEndedAt()));
    doc.append("status", exec.getStatus() == null ? null : exec.getStatus().name());
    doc.append("error_message", exec.getErrorMessage());
    doc.append("error_class", exec.getErrorClass());
    doc.append("duration_ms", exec.getDurationMs());
    return doc;
  }

  public static JobExecutionEntity toJobExecutionEntity(Document doc) {
    JobExecutionEntity exec = new JobExecutionEntity();
    exec.setId(doc.getLong("_id"));
    exec.setJobId(doc.getLong("job_id"));
    exec.setAttempt(doc.getInteger("attempt", 0));
    exec.setNodeId(doc.getString("node_id"));
    exec.setStartedAt(toInstant(doc.getDate("started_at")));
    exec.setEndedAt(toInstant(doc.getDate("ended_at")));
    if (doc.getString("status") != null) {
      exec.setStatus(JobExecutionEntity.ExecutionStatus.valueOf(doc.getString("status")));
    }
    exec.setErrorMessage(doc.getString("error_message"));
    exec.setErrorClass(doc.getString("error_class"));
    exec.setDurationMs(doc.getLong("duration_ms"));
    return exec;
  }

  public static Document toDocument(JobLogEntity logEntry) {
    Document doc = new Document();
    if (logEntry.getId() != null) {
      doc.append("_id", logEntry.getId());
    }
    doc.append("job_id", logEntry.getJobId());
    doc.append("ts", toDate(logEntry.getTs()));
    doc.append("level", logEntry.getLevel() == null ? null : logEntry.getLevel().name());
    doc.append("message", logEntry.getMessage());
    doc.append("mdc", nodeInfoToDocument(logEntry.getMdc()));
    return doc;
  }

  public static Document toDocument(ArchivedJobEntity a) {
    Document doc = new Document();
    if (a.getId() != null) {
      doc.append("_id", a.getId());
    }
    doc.append("original_job_id", a.getOriginalJobId());
    doc.append("final_status", a.getFinalStatus() == null ? null : a.getFinalStatus().name());
    doc.append("job_type", a.getJobType() == null ? null : a.getJobType().name());
    doc.append("priority", a.getPriority() == null ? null : a.getPriority().ordinal());
    doc.append("total_attempts", a.getTotalAttempts());
    doc.append("max_retries", a.getMaxRetries());
    doc.append("backoff_policy", a.getBackoffPolicy() == null ? null : a.getBackoffPolicy().name());
    doc.append("backoff_param_ms", a.getBackoffParamMs());
    doc.append("timeout_sec", a.getTimeoutSec());
    doc.append("target_class", a.getTargetClass());
    doc.append("method_name", a.getMethodName());
    doc.append("business_key", a.getBusinessKey());
    doc.append("cron_expr", a.getCronExpr());
    doc.append("zone_id", a.getZoneId());
    doc.append("original_scheduled_time", toDate(a.getOriginalScheduledTime()));
    doc.append("original_created_at", toDate(a.getOriginalCreatedAt()));
    doc.append("first_execution_time", toDate(a.getFirstExecutionTime()));
    doc.append("completion_time", toDate(a.getCompletionTime()));
    doc.append("total_execution_time_ms", a.getTotalExecutionTimeMs());
    doc.append("queue_wait_ms", a.getQueueWaitMs());
    doc.append("archived_at", toDate(a.getArchivedAt()));
    doc.append("archived_by", a.getArchivedBy());
    doc.append("archive_reason", a.getArchiveReason());
    doc.append("job_result", a.getJobResult());
    doc.append("result_type", a.getResultType());
    doc.append("final_error", a.getFinalError());
    doc.append("payload_summary", a.getPayloadSummary());
    doc.append("depended_on", a.getDependedOn());
    doc.append("superseded_by", a.getSupersededBy());
    doc.append("tags", a.getTags());
    return doc;
  }

  public static ArchivedJobEntity toArchivedJobEntity(Document doc) {
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setId(doc.getLong("_id"));
    a.setOriginalJobId(doc.getLong("original_job_id"));
    if (doc.getString("final_status") != null) {
      a.setFinalStatus(JobStatus.valueOf(doc.getString("final_status")));
    }
    if (doc.getString("job_type") != null) {
      a.setJobType(JobExecutionType.valueOf(doc.getString("job_type")));
    }
    a.setPriority(safeJobPriority(doc.getInteger("priority", 2)));
    a.setTotalAttempts(doc.getInteger("total_attempts", 0));
    a.setMaxRetries(doc.getInteger("max_retries", 0));
    if (doc.getString("backoff_policy") != null) {
      a.setBackoffPolicy(BackoffPolicy.valueOf(doc.getString("backoff_policy")));
    }
    a.setBackoffParamMs(doc.getInteger("backoff_param_ms", 0));
    a.setTimeoutSec(doc.getInteger("timeout_sec", 0));
    a.setTargetClass(doc.getString("target_class"));
    a.setMethodName(doc.getString("method_name"));
    a.setBusinessKey(doc.getString("business_key"));
    a.setCronExpr(doc.getString("cron_expr"));
    a.setZoneId(doc.getString("zone_id"));
    a.setOriginalScheduledTime(toInstant(doc.getDate("original_scheduled_time")));
    a.setOriginalCreatedAt(toInstant(doc.getDate("original_created_at")));
    a.setFirstExecutionTime(toInstant(doc.getDate("first_execution_time")));
    a.setCompletionTime(toInstant(doc.getDate("completion_time")));
    a.setTotalExecutionTimeMs(doc.getLong("total_execution_time_ms"));
    a.setQueueWaitMs(doc.getLong("queue_wait_ms"));
    a.setArchivedAt(toInstant(doc.getDate("archived_at")));
    a.setArchivedBy(doc.getString("archived_by"));
    a.setArchiveReason(doc.getString("archive_reason"));
    a.setJobResult(doc.getString("job_result"));
    a.setResultType(doc.getString("result_type"));
    a.setFinalError(doc.getString("final_error"));
    a.setPayloadSummary(doc.getString("payload_summary"));
    a.setDependedOn(doc.getLong("depended_on"));
    a.setSupersededBy(doc.getLong("superseded_by"));
    a.setTags(doc.getString("tags"));
    return a;
  }

  public static Document toDocument(WorkflowConditionEntity wc) {
    Document doc = new Document();
    if (wc.getId() != null) {
      doc.append("_id", wc.getId());
    }
    doc.append("parent_job_id", wc.getParentJobId());
    doc.append("child_job_id", wc.getChildJobId());
    doc.append(
        "condition_type", wc.getConditionType() == null ? null : wc.getConditionType().name());
    doc.append("condition_expression", wc.getConditionExpression());
    doc.append("condition_priority", wc.getConditionPriority());
    doc.append("created_at", toDate(wc.getCreatedAt()));
    return doc;
  }

  public static WorkflowConditionEntity toWorkflowConditionEntity(Document doc) {
    WorkflowConditionEntity wc = new WorkflowConditionEntity();
    wc.setId(doc.getLong("_id"));
    wc.setParentJobId(doc.getLong("parent_job_id"));
    wc.setChildJobId(doc.getLong("child_job_id"));
    if (doc.getString("condition_type") != null) {
      wc.setConditionType(WorkflowCondition.ConditionType.valueOf(doc.getString("condition_type")));
    }
    wc.setConditionExpression(doc.getString("condition_expression"));
    wc.setConditionPriority(doc.getInteger("condition_priority"));
    wc.setCreatedAt(toInstant(doc.getDate("created_at")));
    return wc;
  }

  public static Document toDocument(BatchMetricsEntity bm) {
    Document doc = new Document();
    doc.append("_id", bm.getBatchId());
    doc.append("total_duration_ms", bm.getTotalDurationMs() == null ? 0L : bm.getTotalDurationMs());
    doc.append(
        "child_execution_ms", bm.getChildExecutionMs() == null ? 0L : bm.getChildExecutionMs());
    doc.append("overhead_ms", bm.getOverheadMs() == null ? 0L : bm.getOverheadMs());
    doc.append("child_count", bm.getChildCount());
    doc.append("success_count", bm.getSuccessCount());
    doc.append("failure_count", bm.getFailureCount());
    doc.append("started_at", toDate(bm.getStartedAt()));
    doc.append("completed_at", toDate(bm.getCompletedAt()));
    doc.append("version", bm.getVersion() == null ? 0 : bm.getVersion());
    return doc;
  }

  public static BatchMetricsEntity toBatchMetricsEntity(Document doc) {
    BatchMetricsEntity bm = new BatchMetricsEntity();
    bm.setBatchId(doc.getLong("_id"));
    bm.setTotalDurationMs(doc.getLong("total_duration_ms"));
    bm.setChildExecutionMs(doc.getLong("child_execution_ms"));
    bm.setOverheadMs(doc.getLong("overhead_ms"));
    bm.setChildCount(doc.getInteger("child_count", 0));
    bm.setSuccessCount(doc.getInteger("success_count", 0));
    bm.setFailureCount(doc.getInteger("failure_count", 0));
    bm.setStartedAt(toInstant(doc.getDate("started_at")));
    bm.setCompletedAt(toInstant(doc.getDate("completed_at")));
    bm.setVersion(doc.getInteger("version", 0));
    return bm;
  }

  public static Document toDocument(DlqAlertEntity alert) {
    Document doc = new Document();
    if (alert.getId() != null) {
      doc.append("_id", alert.getId());
    }
    doc.append("job_id", alert.getJobId());
    doc.append("error_hash", alert.getErrorHash());
    doc.append("alert_sent_at", toDate(alert.getAlertSentAt()));
    doc.append("alert_channel", alert.getAlertChannel());
    return doc;
  }

  public static DlqAlertEntity toDlqAlertEntity(Document doc) {
    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setId(doc.getLong("_id"));
    alert.setJobId(doc.getLong("job_id"));
    alert.setErrorHash(doc.getString("error_hash"));
    alert.setAlertSentAt(toInstant(doc.getDate("alert_sent_at")));
    alert.setAlertChannel(doc.getString("alert_channel"));
    return alert;
  }

  public static Document toDocument(ResourcePermitEntity permit) {
    Document doc = new Document();
    if (permit.getId() != null) {
      doc.append("_id", permit.getId());
    }
    doc.append("resource_name", permit.getResourceName());
    doc.append("job_id", permit.getJobId());
    doc.append("node_id", permit.getNodeId());
    doc.append("acquired_at", toDate(permit.getAcquiredAt()));
    return doc;
  }

  static Document payloadToDocument(JobPayload payload) {
    if (payload == null) {
      return null;
    }
    Document doc = new Document();
    doc.append("target", payload.target());
    doc.append("method", payload.method());
    doc.append("methodDescriptor", payload.methodDescriptor());
    doc.append("isStatic", payload.isStatic());
    doc.append("args", payload.args());
    return doc;
  }

  static JobPayload documentToPayload(Document doc) {
    if (doc == null || doc.isEmpty()) {
      return null;
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) doc.get("args");
    return new JobPayload(
        doc.getString("target"),
        doc.getString("method"),
        doc.getString("methodDescriptor"),
        doc.getBoolean("isStatic", false),
        args);
  }

  static Document paramsToDocument(Map<String, String> params) {
    if (params == null) {
      return new Document();
    }
    Document doc = new Document();
    params.forEach(doc::append);
    return doc;
  }

  static Map<String, String> documentToParams(Document doc) {
    if (doc == null) {
      return Collections.emptyMap();
    }
    Map<String, String> out = new LinkedHashMap<>();
    doc.forEach((k, v) -> out.put(k, v == null ? null : String.valueOf(v)));
    return out;
  }

  private static Document nodeInfoToDocument(Map<String, Object> nodeInfo) {
    if (nodeInfo == null) {
      return new Document();
    }
    Document doc = new Document();
    nodeInfo.forEach(doc::append);
    return doc;
  }

  /**
   * Callers must supply primitives or Strings via {@link #nodeInfoToDocument}; BSON-native types
   * (ObjectId, Date, Decimal128) surface here as their raw Java forms rather than the nested map
   * wrappers that the prior extended-JSON roundtrip produced.
   */
  private static Map<String, Object> documentToNodeInfo(Document doc) {
    return doc == null ? Collections.emptyMap() : new LinkedHashMap<>(doc);
  }

  static Date toDate(Instant instant) {
    return instant == null ? null : Date.from(instant);
  }

  static Instant toInstant(Date date) {
    return date == null ? null : date.toInstant();
  }

  static JobPriority safeJobPriority(int ordinal) {
    JobPriority[] values = JobPriority.values();
    if (ordinal < 0 || ordinal >= values.length) {
      return JobPriority.NORMAL;
    }
    return values[ordinal];
  }
}
