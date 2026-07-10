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
package run.ratchet.store.mongodb;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.converter.PayloadSerializerHolder;
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
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.util.EncryptionIntegrity;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.JobEncryption;
import run.ratchet.store.util.PayloadEncryptor;

/** Bidirectional mapping between Ratchet store-core entities and MongoDB BSON documents. */
public final class DocumentMapper {

  private static final JobStatus DEFAULT_JOB_STATUS = JobStatus.PENDING;
  private static final JobPriority DEFAULT_JOB_PRIORITY = JobPriority.NORMAL;
  private static final String DEFAULT_CRON_EXPR = "";
  private static final String DEFAULT_ZONE_ID = "UTC";
  private static final JobPriority[] JOB_PRIORITY_VALUES = JobPriority.values();
  private static final int DEFAULT_COUNT = 0;
  private static final int DEFAULT_VERSION = 0;
  private static final long DEFAULT_DURATION_MS = 0L;

  private DocumentMapper() {}

  /** Thrown when a MongoDB document cannot be mapped to or from Ratchet store entities. */
  public static final class MappingException extends IllegalArgumentException {
    MappingException(String message) {
      super(message);
    }

    MappingException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static Document toDocument(JobEntity job) {
    Document doc = new Document();
    if (job.getId() != null) {
      doc.append("_id", job.getId());
    }
    doc.append("status", enumNameOrDefault(job.getStatus(), DEFAULT_JOB_STATUS));
    doc.append("paused_from_status", enumName(job.getPausedFromStatus()));
    doc.append("scheduled_time", toDate(job.getScheduledTime()));
    doc.append("job_type", job.getJobType().name());
    doc.append("priority", job.getPriority().ordinal());
    doc.append("attempts", job.getAttempts());
    doc.append("max_retries", job.getMaxRetries());
    doc.append("backoff_policy", job.getBackoffPolicy().name());
    doc.append("backoff_param_ms", job.getBackoffParamMs());
    doc.append("timeout_sec", job.getTimeoutSec());
    doc.append("cron_expr", stringOrDefault(job.getCronExpr(), DEFAULT_CRON_EXPR));
    doc.append("zone_id", stringOrDefault(job.getZoneId(), DEFAULT_ZONE_ID));
    doc.append("next_fire", toDate(job.getNextFire()));
    UUID jobId = job.getId();
    boolean active = JobEncryption.activeFor(job);
    doc.append(
        "payload",
        PayloadEncryptor.encryptArgs(
            payloadToStoredValue(job.getPayload()),
            active,
            EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, jobId)));
    doc.append(
        "params",
        paramsToDocument(
            job.getParams(),
            active,
            EncryptionTarget.rowBound(ProtectedSurface.PARAM_VALUE, jobId)));
    doc.append(
        "target_class",
        job.getPayload() != null ? job.getPayload().target() : job.getTargetClass());
    doc.append(
        "method_name", job.getPayload() != null ? job.getPayload().method() : job.getMethodName());
    doc.append("idempotency_key", job.getIdempotencyKey());
    doc.append("business_key", job.getBusinessKey());
    doc.append("tags", listOrEmpty(job.getTags()));
    doc.append("resource_name", job.getResourceName());
    doc.append("execution_target", job.getExecutionTarget());
    doc.append(
        "on_success_payload",
        PayloadEncryptor.encryptArgs(
            payloadToStoredValue(job.getOnSuccessPayload()),
            active,
            EncryptionTarget.rowBound(ProtectedSurface.ON_SUCCESS_PAYLOAD, jobId)));
    doc.append(
        "on_failure_payload",
        PayloadEncryptor.encryptArgs(
            payloadToStoredValue(job.getOnFailurePayload()),
            active,
            EncryptionTarget.rowBound(ProtectedSurface.ON_FAILURE_PAYLOAD, jobId)));
    doc.append("encrypted_payload", active);
    doc.append("encryption_key_id", JobEncryption.keyId(active));
    doc.append("depends_on", job.getDependsOn());
    doc.append("superseded_by", job.getSupersededBy());
    doc.append("recurring_master_id", job.getRecurringMasterId());
    doc.append("picked_by", job.getPickedBy());
    doc.append("picked_at", toDate(job.getPickedAt()));
    doc.append("last_error", job.getLastError());
    doc.append("created_at", toDate(job.getCreatedAt()));
    doc.append("caller_principal", job.getCallerPrincipal());
    doc.append("trace_context", job.getTraceContext());
    doc.append("updated_at", toDate(job.getUpdatedAt()));
    doc.append("execution_start_time", toDate(job.getExecutionStartTime()));
    doc.append("execution_end_time", toDate(job.getExecutionEndTime()));
    doc.append("execution_duration_ms", job.getExecutionDurationMs());
    doc.append("queue_wait_ms", job.getQueueWaitMs());
    doc.append("job_result", job.getJobResult());
    doc.append("result_type", job.getResultType());
    doc.append("version", versionOrDefault(job.getVersion()));
    doc.append("signal_key", job.getSignalKey());
    doc.append("signal_timeout", toDate(job.getSignalTimeout()));
    doc.append("signal_payload", job.getSignalPayload());
    doc.append("signal_payload_type", job.getSignalPayloadType());
    doc.append("signal_outcome", job.getSignalOutcome());
    doc.append("signal_rejection_reason", job.getSignalRejectionReason());
    doc.append("signal_delivered_at", toDate(job.getSignalDeliveredAt()));
    doc.append("signal_delivered_by", job.getSignalDeliveredBy());
    doc.append("signal_delivery_id", job.getSignalDeliveryId());
    return doc;
  }

  public static JobEntity toJobEntity(Document doc) {
    JobEntity job = new JobEntity();
    job.setId(doc.get("_id", UUID.class));
    job.setStatus(requiredEnumValue(doc, "status", JobStatus.class));
    if (doc.getString("paused_from_status") != null) {
      job.setPausedFromStatus(enumValue(doc.getString("paused_from_status"), JobStatus.class));
    }
    job.setScheduledTime(toInstant(doc.getDate("scheduled_time")));
    job.setJobType(requiredEnumValue(doc, "job_type", JobExecutionType.class));
    job.setPriority(jobPriorityFromOrdinal(doc.getInteger("priority")));
    job.setAttempts(doc.getInteger("attempts", DEFAULT_COUNT));
    job.setMaxRetries(doc.getInteger("max_retries", DEFAULT_COUNT));
    job.setBackoffPolicy(requiredEnumValue(doc, "backoff_policy", BackoffPolicy.class));
    job.setBackoffParamMs(doc.getInteger("backoff_param_ms", DEFAULT_COUNT));
    job.setTimeoutSec(doc.getInteger("timeout_sec", DEFAULT_COUNT));
    job.setCronExpr(doc.getString("cron_expr"));
    job.setZoneId(doc.getString("zone_id"));
    job.setNextFire(toInstant(doc.getDate("next_fire")));
    job.setEncryptedPayload(doc.getBoolean("encrypted_payload", false));
    UUID readJobId = job.getId();
    Object rawPayload = doc.get("payload");
    if (job.isEncryptedPayload()
        && rawPayload instanceof String s
        && PayloadEncryptor.argsFlaggedButUnframed(s)) {
      EncryptionIntegrity.flaggedButUnframed(readJobId, ProtectedSurface.PAYLOAD_ARGS);
    }
    job.setPayload(
        storedValueToPayload(
            decryptArgsValue(
                rawPayload, EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, readJobId))));
    job.setParams(
        documentToParams(
            doc.get("params", Document.class),
            EncryptionTarget.rowBound(ProtectedSurface.PARAM_VALUE, readJobId)));
    job.setTargetClass(doc.getString("target_class"));
    job.setMethodName(doc.getString("method_name"));
    job.setIdempotencyKey(doc.getString("idempotency_key"));
    job.setBusinessKey(doc.getString("business_key"));
    job.setTags(doc.getList("tags", String.class));
    job.setResourceName(doc.getString("resource_name"));
    job.setExecutionTarget(doc.getString("execution_target"));
    job.setOnSuccessPayload(
        storedValueToPayload(
            decryptArgsValue(
                doc.get("on_success_payload"),
                EncryptionTarget.rowBound(ProtectedSurface.ON_SUCCESS_PAYLOAD, readJobId))));
    job.setOnFailurePayload(
        storedValueToPayload(
            decryptArgsValue(
                doc.get("on_failure_payload"),
                EncryptionTarget.rowBound(ProtectedSurface.ON_FAILURE_PAYLOAD, readJobId))));
    job.setDependsOn(doc.get("depends_on", UUID.class));
    job.setSupersededBy(doc.get("superseded_by", UUID.class));
    job.setRecurringMasterId(doc.get("recurring_master_id", UUID.class));
    job.setPickedBy(doc.getString("picked_by"));
    job.setPickedAt(toInstant(doc.getDate("picked_at")));
    job.setLastError(doc.getString("last_error"));
    job.setCreatedAt(toInstant(doc.getDate("created_at")));
    job.setCallerPrincipal(doc.getString("caller_principal"));
    job.setTraceContext(documentToStringMap(doc.get("trace_context")));
    job.setUpdatedAt(toInstant(doc.getDate("updated_at")));
    job.setExecutionStartTime(toInstant(doc.getDate("execution_start_time")));
    job.setExecutionEndTime(toInstant(doc.getDate("execution_end_time")));
    job.setExecutionDurationMs(doc.getLong("execution_duration_ms"));
    job.setQueueWaitMs(doc.getLong("queue_wait_ms"));
    job.setJobResult(doc.getString("job_result"));
    job.setResultType(doc.getString("result_type"));
    job.setVersion(doc.getInteger("version", DEFAULT_VERSION));
    job.setSignalKey(doc.getString("signal_key"));
    job.setSignalTimeout(toInstant(doc.getDate("signal_timeout")));
    job.setSignalPayload(doc.getString("signal_payload"));
    job.setSignalPayloadType(doc.getString("signal_payload_type"));
    job.setSignalOutcome(doc.getString("signal_outcome"));
    job.setSignalRejectionReason(doc.getString("signal_rejection_reason"));
    job.setSignalDeliveredAt(toInstant(doc.getDate("signal_delivered_at")));
    job.setSignalDeliveredBy(doc.getString("signal_delivered_by"));
    job.setSignalDeliveryId(doc.getString("signal_delivery_id"));
    return job;
  }

  public static JobClaimDto toJobClaimDto(Document doc) {
    return new JobClaimDto(
        doc.get("_id", UUID.class),
        requiredEnumValue(doc, "status", JobStatus.class),
        requiredEnumValue(doc, "job_type", JobExecutionType.class),
        jobPriorityFromOrdinal(doc.getInteger("priority")),
        toInstant(doc.getDate("scheduled_time")),
        doc.getInteger("version"),
        doc.getInteger("timeout_sec", DEFAULT_COUNT),
        doc.getString("picked_by"),
        toInstant(doc.getDate("picked_at")),
        doc.getString("business_key"),
        doc.getInteger("attempts", DEFAULT_COUNT),
        doc.getInteger("max_retries", DEFAULT_COUNT),
        doc.getString("execution_target"));
  }

  public static Document toRecurringDocument(RecurringJobDefinition definition) {
    UUID id = definition.id();
    boolean active = JobEncryption.activeFor(definition.encryptedPayload());
    Document doc = new Document();
    doc.append("_id", id);
    doc.append("priority", definition.priority());
    doc.append("max_retries", definition.maxRetries());
    doc.append(
        "backoff_policy",
        definition.backoffPolicy() != null ? definition.backoffPolicy().name() : "NONE");
    doc.append("backoff_param_ms", definition.backoffParamMs());
    doc.append("timeout_sec", definition.timeoutSec());
    doc.append("cron_expr", definition.cronExpr());
    doc.append("zone_id", stringOrDefault(definition.zoneId(), DEFAULT_ZONE_ID));
    doc.append("next_fire", toDate(definition.nextFire()));
    doc.append("is_paused", definition.paused());
    doc.append("paused_at", toDate(definition.pausedAt()));
    doc.append(
        "payload",
        encryptedRecurringColumn(definition.payload(), active, ProtectedSurface.PAYLOAD_ARGS, id));
    doc.append(
        "on_success_payload",
        encryptedRecurringColumn(
            definition.onSuccessPayload(), active, ProtectedSurface.ON_SUCCESS_PAYLOAD, id));
    doc.append(
        "on_failure_payload",
        encryptedRecurringColumn(
            definition.onFailurePayload(), active, ProtectedSurface.ON_FAILURE_PAYLOAD, id));
    doc.append("business_key", definition.businessKey());
    doc.append("resource_name", definition.resourceName());
    doc.append("execution_target", definition.executionTarget());
    doc.append(
        "created_at",
        toDate(definition.createdAt() != null ? definition.createdAt() : Instant.now()));
    doc.append("caller_principal", definition.callerPrincipal());
    doc.append("encrypted_payload", active);
    return doc;
  }

  public static RecurringJobDefinition toRecurringJobDefinition(Document doc) {
    Number priority = (Number) doc.get("priority");
    Number maxRetries = (Number) doc.get("max_retries");
    Number backoffParamMs = (Number) doc.get("backoff_param_ms");
    Number timeoutSec = (Number) doc.get("timeout_sec");
    BackoffPolicy backoffPolicy =
        BackoffPolicy.valueOf(
            doc.getString("backoff_policy") != null ? doc.getString("backoff_policy") : "NONE");
    UUID id = doc.get("_id", UUID.class);
    Object rawPayload = doc.get("payload");
    if (doc.getBoolean("encrypted_payload", false)
        && rawPayload instanceof String s
        && PayloadEncryptor.argsFlaggedButUnframed(s)) {
      EncryptionIntegrity.flaggedButUnframed(id, ProtectedSurface.PAYLOAD_ARGS);
    }
    return new RecurringJobDefinition(
        id,
        doc.getString("cron_expr"),
        doc.getString("zone_id"),
        toInstant(doc.getDate("next_fire")),
        doc.getBoolean("is_paused", false),
        toInstant(doc.getDate("paused_at")),
        priority != null ? priority.intValue() : DEFAULT_JOB_PRIORITY.ordinal(),
        maxRetries != null ? maxRetries.intValue() : DEFAULT_COUNT,
        backoffPolicy,
        backoffParamMs != null ? backoffParamMs.intValue() : DEFAULT_COUNT,
        timeoutSec != null ? timeoutSec.intValue() : DEFAULT_COUNT,
        storedValueToPayload(
            decryptArgsValue(
                rawPayload, EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, id))),
        storedValueToPayload(
            decryptArgsValue(
                doc.get("on_success_payload"),
                EncryptionTarget.rowBound(ProtectedSurface.ON_SUCCESS_PAYLOAD, id))),
        storedValueToPayload(
            decryptArgsValue(
                doc.get("on_failure_payload"),
                EncryptionTarget.rowBound(ProtectedSurface.ON_FAILURE_PAYLOAD, id))),
        doc.getString("business_key"),
        doc.getString("resource_name"),
        doc.getString("execution_target"),
        toInstant(doc.getDate("created_at")),
        doc.getString("caller_principal"),
        doc.getBoolean("encrypted_payload", false));
  }

  public static Document toDocument(BatchEntity batch) {
    Document doc = new Document();
    doc.append("_id", batch.getId());
    doc.append("total_items", batch.getTotalItems());
    doc.append("completed_items", batch.getCompletedItems());
    doc.append("failed_items", batch.getFailedItems());
    doc.append("completion_processed", batch.getCompletionProcessed());
    doc.append("version", versionOrDefault(batch.getVersion()));
    doc.append("progress_hook", payloadToStoredValue(batch.getProgressHook()));
    return doc;
  }

  public static BatchEntity toBatchEntity(Document doc) {
    BatchEntity batch = new BatchEntity();
    batch.setId(doc.get("_id", UUID.class));
    batch.setTotalItems(doc.getInteger("total_items", DEFAULT_COUNT));
    batch.setCompletedItems(doc.getInteger("completed_items", DEFAULT_COUNT));
    batch.setFailedItems(doc.getInteger("failed_items", DEFAULT_COUNT));
    batch.setCompletionProcessed(doc.getBoolean("completion_processed", false));
    batch.setVersion(doc.getInteger("version", DEFAULT_VERSION));
    batch.setProgressHook(storedValueToPayload(doc.get("progress_hook")));
    return batch;
  }

  public static BatchProgress toBatchProgress(Document doc, UUID batchId) {
    return new BatchProgress(
        batchId,
        doc.getInteger("total_items", DEFAULT_COUNT),
        doc.getInteger("completed_items", DEFAULT_COUNT),
        doc.getInteger("failed_items", DEFAULT_COUNT),
        storedValueToPayload(doc.get("progress_hook")));
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
    doc.append("status", enumName(exec.getStatus()));
    doc.append("error_message", exec.getErrorMessage());
    doc.append("error_class", exec.getErrorClass());
    doc.append("duration_ms", exec.getDurationMs());
    return doc;
  }

  public static JobExecutionEntity toJobExecutionEntity(Document doc) {
    JobExecutionEntity exec = new JobExecutionEntity();
    exec.setId(doc.get("_id", UUID.class));
    exec.setJobId(doc.get("job_id", UUID.class));
    exec.setAttempt(doc.getInteger("attempt", DEFAULT_COUNT));
    exec.setNodeId(doc.getString("node_id"));
    exec.setStartedAt(toInstant(doc.getDate("started_at")));
    exec.setEndedAt(toInstant(doc.getDate("ended_at")));
    if (doc.getString("status") != null) {
      exec.setStatus(enumValue(doc.getString("status"), JobExecutionEntity.ExecutionStatus.class));
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
    doc.append("level", enumName(logEntry.getLevel()));
    doc.append("message", logEntry.getMessage());
    doc.append("mdc", nodeInfoToDocument(logEntry.getMdc()));
    return doc;
  }

  public static JobLogEntity toJobLogEntity(Document doc) {
    JobLogEntity logEntry =
        new JobLogEntity(
            doc.get("job_id", UUID.class),
            toInstant(doc.getDate("ts")),
            requiredEnumValue(doc, "level", JobLogEntity.LogLevel.class),
            doc.getString("message"),
            documentToNodeInfo(doc.get("mdc", Document.class)));
    logEntry.setId(doc.get("_id", UUID.class));
    return logEntry;
  }

  public static Document toDocument(ArchivedJobEntity a) {
    Document doc = new Document();
    if (a.getId() != null) {
      doc.append("_id", a.getId());
    }
    doc.append("original_job_id", a.getOriginalJobId());
    doc.append("final_status", enumName(a.getFinalStatus()));
    doc.append("job_type", enumName(a.getJobType()));
    doc.append("priority", a.getPriority() == null ? null : a.getPriority().ordinal());
    doc.append("total_attempts", a.getTotalAttempts());
    doc.append("max_retries", a.getMaxRetries());
    doc.append("backoff_policy", enumName(a.getBackoffPolicy()));
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
    doc.append("properties", a.getProperties());
    doc.append("extension_state", a.getExtensionState());
    return doc;
  }

  /**
   * Maps an archive document from {@code scheduler_job_archive} to a {@link JobEntity} suitable for
   * inclusion in dashboard query results. Fields absent from the archive are left null or
   * zero-valued. Uses archive-specific field names (e.g. {@code original_job_id}, {@code
   * final_status}, {@code first_execution_time}) that differ from the live collection.
   */
  public static JobEntity archivedDocToJobEntity(Document doc) {
    if (doc == null) {
      return null;
    }
    JobEntity e = new JobEntity();
    e.setId(doc.get("original_job_id", UUID.class));
    String finalStatus = doc.getString("final_status");
    if (finalStatus != null) {
      e.setStatus(enumValue(finalStatus, JobStatus.class));
    }
    String jobType = doc.getString("job_type");
    if (jobType != null) {
      e.setJobType(enumValue(jobType, JobExecutionType.class));
    }
    e.setPriority(jobPriorityFromOrdinal(doc.getInteger("priority")));
    e.setMaxRetries(doc.getInteger("max_retries", DEFAULT_COUNT));
    if (doc.getString("backoff_policy") != null) {
      e.setBackoffPolicy(enumValue(doc.getString("backoff_policy"), BackoffPolicy.class));
    }
    e.setBackoffParamMs(doc.getInteger("backoff_param_ms", DEFAULT_COUNT));
    e.setTimeoutSec(doc.getInteger("timeout_sec", DEFAULT_COUNT));
    e.setTargetClass(doc.getString("target_class"));
    e.setMethodName(doc.getString("method_name"));
    e.setBusinessKey(doc.getString("business_key"));
    e.setCronExpr(doc.getString("cron_expr"));
    e.setZoneId(doc.getString("zone_id"));
    e.setDependsOn(doc.get("depended_on", UUID.class));
    e.setSupersededBy(doc.get("superseded_by", UUID.class));
    e.setCreatedAt(toInstant(doc.getDate("original_created_at")));
    e.setScheduledTime(toInstant(doc.getDate("original_scheduled_time")));
    e.setExecutionStartTime(toInstant(doc.getDate("first_execution_time")));
    e.setUpdatedAt(toInstant(doc.getDate("archived_at")));
    e.setExecutionDurationMs(doc.getLong("total_execution_time_ms"));
    e.setQueueWaitMs(doc.getLong("queue_wait_ms"));
    e.setJobResult(doc.getString("job_result"));
    e.setResultType(doc.getString("result_type"));
    e.setLastError(doc.getString("final_error"));
    e.setAttempts(doc.getInteger("total_attempts", DEFAULT_COUNT));
    return e;
  }

  public static ArchivedJobEntity toArchivedJobEntity(Document doc) {
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setId(doc.get("_id", UUID.class));
    a.setOriginalJobId(doc.get("original_job_id", UUID.class));
    if (doc.getString("final_status") != null) {
      a.setFinalStatus(enumValue(doc.getString("final_status"), JobStatus.class));
    }
    if (doc.getString("job_type") != null) {
      a.setJobType(enumValue(doc.getString("job_type"), JobExecutionType.class));
    }
    a.setPriority(jobPriorityFromOrdinal(doc.getInteger("priority")));
    a.setTotalAttempts(doc.getInteger("total_attempts", DEFAULT_COUNT));
    a.setMaxRetries(doc.getInteger("max_retries", DEFAULT_COUNT));
    if (doc.getString("backoff_policy") != null) {
      a.setBackoffPolicy(enumValue(doc.getString("backoff_policy"), BackoffPolicy.class));
    }
    a.setBackoffParamMs(doc.getInteger("backoff_param_ms", DEFAULT_COUNT));
    a.setTimeoutSec(doc.getInteger("timeout_sec", DEFAULT_COUNT));
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
    a.setDependedOn(doc.get("depended_on", UUID.class));
    a.setSupersededBy(doc.get("superseded_by", UUID.class));
    a.setTags(doc.getString("tags"));
    a.setProperties(doc.getString("properties"));
    a.setExtensionState(doc.getString("extension_state"));
    return a;
  }

  public static Document toDocument(WorkflowConditionEntity wc) {
    Document doc = new Document();
    if (wc.getId() != null) {
      doc.append("_id", wc.getId());
    }
    doc.append("parent_job_id", wc.getParentJobId());
    doc.append("child_job_id", wc.getChildJobId());
    doc.append("condition_type", enumName(wc.getConditionType()));
    doc.append("condition_expression", wc.getConditionExpression());
    doc.append("condition_priority", wc.getConditionPriority());
    doc.append("created_at", toDate(wc.getCreatedAt()));
    return doc;
  }

  public static WorkflowConditionEntity toWorkflowConditionEntity(Document doc) {
    WorkflowConditionEntity wc = new WorkflowConditionEntity();
    wc.setId(doc.get("_id", UUID.class));
    wc.setParentJobId(doc.get("parent_job_id", UUID.class));
    wc.setChildJobId(doc.get("child_job_id", UUID.class));
    if (doc.getString("condition_type") != null) {
      wc.setConditionType(
          enumValue(doc.getString("condition_type"), WorkflowCondition.ConditionType.class));
    }
    wc.setConditionExpression(doc.getString("condition_expression"));
    wc.setConditionPriority(doc.getInteger("condition_priority"));
    wc.setCreatedAt(toInstant(doc.getDate("created_at")));
    return wc;
  }

  public static Document toDocument(BatchMetricsEntity bm) {
    Document doc = new Document();
    doc.append("_id", bm.getBatchId());
    doc.append("total_duration_ms", longOrDefault(bm.getTotalDurationMs()));
    doc.append("child_execution_ms", longOrDefault(bm.getChildExecutionMs()));
    doc.append("overhead_ms", longOrDefault(bm.getOverheadMs()));
    doc.append("child_count", bm.getChildCount());
    doc.append("success_count", bm.getSuccessCount());
    doc.append("failure_count", bm.getFailureCount());
    doc.append("started_at", toDate(bm.getStartedAt()));
    doc.append("completed_at", toDate(bm.getCompletedAt()));
    doc.append("version", versionOrDefault(bm.getVersion()));
    return doc;
  }

  public static BatchMetricsEntity toBatchMetricsEntity(Document doc) {
    BatchMetricsEntity bm = new BatchMetricsEntity();
    bm.setBatchId(doc.get("_id", UUID.class));
    bm.setTotalDurationMs(doc.getLong("total_duration_ms"));
    bm.setChildExecutionMs(doc.getLong("child_execution_ms"));
    bm.setOverheadMs(doc.getLong("overhead_ms"));
    bm.setChildCount(doc.getInteger("child_count", DEFAULT_COUNT));
    bm.setSuccessCount(doc.getInteger("success_count", DEFAULT_COUNT));
    bm.setFailureCount(doc.getInteger("failure_count", DEFAULT_COUNT));
    bm.setStartedAt(toInstant(doc.getDate("started_at")));
    bm.setCompletedAt(toInstant(doc.getDate("completed_at")));
    bm.setVersion(doc.getInteger("version", DEFAULT_VERSION));
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
    alert.setId(doc.get("_id", UUID.class));
    alert.setJobId(doc.get("job_id", UUID.class));
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

  public static ResourcePermitEntity toResourcePermitEntity(Document doc) {
    ResourcePermitEntity permit = new ResourcePermitEntity();
    permit.setId(doc.get("_id", UUID.class));
    permit.setResourceName(doc.getString("resource_name"));
    permit.setJobId(doc.get("job_id", UUID.class));
    permit.setNodeId(doc.getString("node_id"));
    permit.setAcquiredAt(toInstant(doc.getDate("acquired_at")));
    return permit;
  }

  static String payloadToStoredValue(JobPayload payload) {
    if (payload == null) {
      return null;
    }
    try {
      return PayloadSerializerHolder.get().serialize(payload);
    } catch (RuntimeException e) {
      throw new MappingException("Could not serialize MongoDB job payload", e);
    }
  }

  static JobPayload storedValueToPayload(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String json) {
      try {
        return json.isEmpty()
            ? null
            : PayloadSerializerHolder.get().deserialize(json, JobPayload.class);
      } catch (RuntimeException e) {
        throw new MappingException("Could not deserialize MongoDB job payload", e);
      }
    }
    if (value instanceof Document doc) {
      return documentToPayload(doc);
    }
    throw new MappingException(
        "Unsupported MongoDB job payload type: " + value.getClass().getSimpleName());
  }

  /**
   * Decrypts a stored job-payload value when it is a framed string; a {@link Document}-shaped value
   * (no frame) or plaintext passes through for {@link #storedValueToPayload} to handle.
   */
  private static Object decryptArgsValue(Object value, EncryptionTarget target) {
    return value instanceof String s ? PayloadEncryptor.decryptArgs(s, target) : value;
  }

  /**
   * Encrypts a recurring-master payload template for storage, binding the ciphertext to the
   * master's id under {@code surface} — the same per-surface binding the equivalent live-job column
   * uses. No-op when {@code active} is false or the payload is null/argument-free.
   */
  static String encryptedRecurringColumn(
      JobPayload payload, boolean active, ProtectedSurface surface, UUID masterId) {
    return PayloadEncryptor.encryptArgs(
        payloadToStoredValue(payload), active, EncryptionTarget.rowBound(surface, masterId));
  }

  private static JobPayload documentToPayload(Document doc) {
    if (doc == null || doc.isEmpty()) {
      return null;
    }
    List<Object> args = documentToArgs(doc.get("args"));
    return new JobPayload(
        doc.getString("target"),
        doc.getString("method"),
        doc.getString("methodDescriptor"),
        doc.getBoolean("isStatic", false),
        args);
  }

  static Document paramsToDocument(
      Map<String, String> params, boolean active, EncryptionTarget target) {
    if (params == null) {
      return new Document();
    }
    Document doc = new Document();
    // Encrypt each parameter value at rest (keys stay cleartext); no-op when inactive.
    params.forEach((k, v) -> doc.append(k, PayloadEncryptor.encryptValue(v, active, target)));
    return doc;
  }

  static Map<String, String> documentToParams(Document doc, EncryptionTarget target) {
    if (doc == null) {
      return Collections.emptyMap();
    }
    Map<String, String> out = new LinkedHashMap<>();
    doc.forEach(
        (k, v) ->
            out.put(
                k, v == null ? null : PayloadEncryptor.decryptValue(String.valueOf(v), target)));
    return out;
  }

  private static Map<String, String> documentToStringMap(Object value) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof Map<?, ?> raw)) {
      throw new IllegalArgumentException("Expected MongoDB document map, got: " + value.getClass());
    }
    Map<String, String> out = new LinkedHashMap<>();
    raw.forEach(
        (k, v) -> {
          if (!(k instanceof String key)) {
            throw new IllegalArgumentException("MongoDB document map key must be a String: " + k);
          }
          out.put(key, v == null ? null : String.valueOf(v));
        });
    return out;
  }

  private static List<Object> documentToArgs(Object value) {
    if (value == null) {
      return List.of();
    }
    if (value instanceof List<?> args) {
      return List.copyOf(args);
    }
    throw new IllegalArgumentException(
        "Expected MongoDB payload args list, got: " + value.getClass());
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
    return instant == null ? null : Date.from(truncateToMillis(instant));
  }

  /**
   * Truncates to millisecond precision, matching what a BSON {@code Date} can store. Persisting an
   * Instant with sub-millisecond nanos and reading it back otherwise returns an earlier value,
   * which would let a claim fire up to a millisecond early. Callers persisting an entity should
   * truncate the entity's own Instant fields with this so the in-memory object matches storage.
   */
  static Instant truncateToMillis(Instant instant) {
    return instant == null ? null : instant.truncatedTo(ChronoUnit.MILLIS);
  }

  static Instant toInstant(Date date) {
    return date == null ? null : date.toInstant();
  }

  private static String enumName(Enum<?> value) {
    return value == null ? null : value.name();
  }

  private static String enumNameOrDefault(Enum<?> value, Enum<?> defaultValue) {
    return enumName(value == null ? defaultValue : value);
  }

  private static <E extends Enum<E>> E enumValue(String value, Class<E> enumType) {
    return value == null ? null : Enum.valueOf(enumType, value);
  }

  private static <E extends Enum<E>> E requiredEnumValue(
      Document doc, String fieldName, Class<E> enumType) {
    String value = doc.getString(fieldName);
    if (value == null) {
      throw new IllegalArgumentException(
          "Required enum field '" + fieldName + "' is null for " + enumType.getSimpleName());
    }
    try {
      return Enum.valueOf(enumType, value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid enum value '"
              + value
              + "' for required field '"
              + fieldName
              + "' ("
              + enumType.getSimpleName()
              + ")",
          e);
    }
  }

  private static String stringOrDefault(String value, String defaultValue) {
    return value == null ? defaultValue : value;
  }

  private static int versionOrDefault(Integer version) {
    return version == null ? DEFAULT_VERSION : version;
  }

  private static long longOrDefault(Long value) {
    return value == null ? DEFAULT_DURATION_MS : value;
  }

  private static <T> List<T> listOrEmpty(List<T> value) {
    return value == null ? List.of() : value;
  }

  private static JobPriority jobPriorityFromOrdinal(Integer ordinal) {
    return safeJobPriority(ordinal == null ? DEFAULT_JOB_PRIORITY.ordinal() : ordinal);
  }

  static JobPriority safeJobPriority(int ordinal) {
    if (ordinal < 0 || ordinal >= JOB_PRIORITY_VALUES.length) {
      return JobPriority.NORMAL;
    }
    return JOB_PRIORITY_VALUES[ordinal];
  }
}
