package run.ratchet.store.entity;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.id.UuidV7EntityListener;

/** Persisted record of a scheduled task. @see JobStatus @see JobExecutionType */
@Entity
@Table(
    name = "scheduler_job",
    indexes = {
      @Index(name = "idx_job_due", columnList = "status, scheduled_time"),
      @Index(name = "idx_job_priority_due", columnList = "priority, scheduled_time"),
      @Index(name = "idx_job_picked_by", columnList = "picked_by"),
      @Index(name = "idx_target_class", columnList = "target_class"),
      @Index(name = "idx_method_name", columnList = "method_name"),
      @Index(name = "idx_job_poll_composite", columnList = "status, priority, scheduled_time"),
      @Index(
          name = "idx_job_claim_cover",
          columnList = "status, job_type, priority, scheduled_time, job_id"),
      @Index(name = "idx_job_type", columnList = "job_type"),
      @Index(name = "idx_job_depends_on", columnList = "depends_on"),
      @Index(name = "idx_job_superseded_by", columnList = "superseded_by"),
      @Index(name = "idx_job_business_key", columnList = "business_key"),
      @Index(name = "idx_job_created_at", columnList = "created_at"),
      @Index(name = "idx_job_updated_at", columnList = "updated_at"),
      @Index(name = "idx_signal_key_status", columnList = "signal_key, status"),
      @Index(name = "idx_signal_timeout_status", columnList = "status, signal_timeout")
    })
@EntityListeners(UuidV7EntityListener.class)
public class JobEntity implements UuidV7EntityListener.UuidV7Assignable {

  @Id
  @Column(name = "job_id")
  private UUID id;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 16)
  private JobStatus status = JobStatus.PENDING;

  @Enumerated(EnumType.STRING)
  @Column(name = "paused_from_status", length = 20)
  private JobStatus pausedFromStatus;

  @Column(name = "scheduled_time", nullable = false)
  private Instant scheduledTime;

  @Enumerated(EnumType.STRING)
  @Column(name = "job_type", nullable = false, length = 16)
  private JobExecutionType jobType;

  @Enumerated(EnumType.ORDINAL)
  @Column(nullable = false)
  private JobPriority priority = JobPriority.NORMAL;

  @Column(nullable = false)
  private int attempts = 0;

  @Column(name = "max_retries", nullable = false)
  private int maxRetries = 0;

  @Enumerated(EnumType.STRING)
  @Column(name = "backoff_policy", nullable = false, length = 16)
  private BackoffPolicy backoffPolicy = BackoffPolicy.NONE;

  @Column(name = "backoff_param_ms", nullable = false)
  private int backoffParamMs = 0;

  @Column(name = "timeout_sec", nullable = false)
  private int timeoutSec = 0;

  @Column(name = "cron_expr", length = 64, nullable = false)
  private String cronExpr = "";

  @Column(name = "zone_id", length = 32, nullable = false)
  private String zoneId = "UTC";

  // scheduler_job does not carry a next_fire column — that anchor lives on
  // scheduler_recurring_job. The field stays as a transient carrier for the authorization-policy
  // gate, which inspects next_fire on the transient recurring entity it builds for the type-aware
  // policy check.
  @jakarta.persistence.Transient private Instant nextFire;

  @Convert(converter = JobPayloadConverter.class)
  @Column(nullable = false)
  private JobPayload payload;

  @Convert(converter = JsonMapConverter.class)
  private Map<String, String> params;

  @Convert(converter = JsonMapConverter.class)
  @Column(name = "trace_context")
  private Map<String, String> traceContext;

  // target_class and method_name are populated outside JPA so they remain queryable for
  // dashboards without round-tripping the JobPayload converter. Store implementations must
  // populate these columns through a DDL-level generated column or a native INSERT path —
  // a JPA-only insert that respects these annotations would leave the columns NULL and
  // break idx_target_class / idx_method_name.
  @Column(name = "target_class", insertable = false, updatable = false)
  private String targetClass;

  @Column(name = "method_name", insertable = false, updatable = false)
  private String methodName;

  @Column(name = "idempotency_key", nullable = false, unique = true, length = 36)
  private String idempotencyKey;

  @Column(name = "business_key")
  private String businessKey;

  @ElementCollection(fetch = FetchType.LAZY)
  @CollectionTable(
      name = "scheduler_job_tag",
      joinColumns = @JoinColumn(name = "job_id"),
      indexes = @Index(name = "idx_job_tag_tag_job", columnList = "tag, job_id"))
  @Column(name = "tag", length = 64)
  private List<String> tags;

  @Column(name = "resource_name", length = 100)
  private String resourceName;

  @Convert(converter = JobPayloadConverter.class)
  @Column(name = "on_success_payload")
  private JobPayload onSuccessPayload;

  @Convert(converter = JobPayloadConverter.class)
  @Column(name = "on_failure_payload")
  private JobPayload onFailurePayload;

  @Column(name = "depends_on")
  private UUID dependsOn;

  @Column(name = "superseded_by")
  private UUID supersededBy;

  /**
   * Set on child rows spawned by a recurring master. Points to the master's id in {@code
   * scheduler_recurring_job} via FK with {@code ON DELETE SET NULL}.
   */
  @Column(name = "recurring_master_id")
  private UUID recurringMasterId;

  @Column(name = "picked_by", length = 64)
  private String pickedBy;

  @Column(name = "picked_at")
  private Instant pickedAt;

  @Column(name = "last_error")
  private String lastError;

  @Column(name = "created_at", updatable = false)
  private Instant createdAt;

  @Column(name = "caller_principal", updatable = false, length = 255)
  private String callerPrincipal;

  @Column(name = "updated_at")
  private Instant updatedAt;

  @Column(name = "execution_start_time")
  private Instant executionStartTime;

  @Column(name = "execution_end_time")
  private Instant executionEndTime;

  @Column(name = "execution_duration_ms")
  private Long executionDurationMs;

  @Column(name = "queue_wait_ms")
  private Long queueWaitMs;

  @Column(name = "job_result")
  private String jobResult;

  @Column(name = "result_type", length = 100)
  private String resultType;

  @Version
  @Column(name = "version")
  private Integer version;

  @Column(name = "signal_key", length = 255)
  private String signalKey;

  @Column(name = "signal_timeout")
  private Instant signalTimeout;

  @Column(name = "signal_payload", columnDefinition = "TEXT")
  private String signalPayload;

  @Column(name = "signal_payload_type", length = 16)
  private String signalPayloadType;

  @Column(name = "signal_outcome", length = 32)
  private String signalOutcome;

  @Column(name = "signal_rejection_reason", columnDefinition = "TEXT")
  private String signalRejectionReason;

  @Column(name = "signal_delivered_at")
  private Instant signalDeliveredAt;

  @Column(name = "signal_delivered_by", length = 255)
  private String signalDeliveredBy;

  @Column(name = "signal_delivery_id", length = 36)
  private String signalDeliveryId;

  // populated by MysqlJobStore hydrator from cold.terminal_status; never persisted via JPA
  // (PG schema does not have the column yet — added in CP3).
  @Transient private JobStatus terminalStatus;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public JobStatus getPausedFromStatus() {
    return pausedFromStatus;
  }

  public void setPausedFromStatus(JobStatus pausedFromStatus) {
    this.pausedFromStatus = pausedFromStatus;
  }

  public Instant getScheduledTime() {
    return scheduledTime;
  }

  public void setScheduledTime(Instant scheduledTime) {
    this.scheduledTime = scheduledTime;
  }

  public JobExecutionType getJobType() {
    return jobType;
  }

  public void setJobType(JobExecutionType jobType) {
    this.jobType = jobType;
  }

  public JobType getPublicJobType() {
    return jobType != null ? jobType.toPublicType() : null;
  }

  public JobPriority getPriority() {
    return priority;
  }

  public void setPriority(JobPriority priority) {
    this.priority = priority;
  }

  public int getAttempts() {
    return attempts;
  }

  public void setAttempts(int attempts) {
    this.attempts = attempts;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public BackoffPolicy getBackoffPolicy() {
    return backoffPolicy;
  }

  public void setBackoffPolicy(BackoffPolicy backoffPolicy) {
    this.backoffPolicy = backoffPolicy;
  }

  public int getBackoffParamMs() {
    return backoffParamMs;
  }

  public void setBackoffParamMs(int backoffParamMs) {
    this.backoffParamMs = backoffParamMs;
  }

  public int getTimeoutSec() {
    return timeoutSec;
  }

  public void setTimeoutSec(int timeoutSec) {
    this.timeoutSec = timeoutSec;
  }

  public String getCronExpr() {
    return cronExpr;
  }

  public void setCronExpr(String cronExpr) {
    this.cronExpr = cronExpr;
  }

  public String getZoneId() {
    return zoneId;
  }

  public void setZoneId(String zoneId) {
    this.zoneId = zoneId;
  }

  public Instant getNextFire() {
    return nextFire;
  }

  public void setNextFire(Instant nextFire) {
    this.nextFire = nextFire;
  }

  public JobPayload getPayload() {
    return payload;
  }

  public void setPayload(JobPayload payload) {
    this.payload = payload;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public Map<String, String> getTraceContext() {
    return traceContext;
  }

  public void setTraceContext(Map<String, String> traceContext) {
    this.traceContext = traceContext;
  }

  public String getTargetClass() {
    return targetClass;
  }

  public void setTargetClass(String targetClass) {
    this.targetClass = targetClass;
  }

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  public String getIdempotencyKey() {
    return idempotencyKey;
  }

  public void setIdempotencyKey(String idempotencyKey) {
    this.idempotencyKey = idempotencyKey;
  }

  public String getBusinessKey() {
    return businessKey;
  }

  public void setBusinessKey(String businessKey) {
    this.businessKey = businessKey;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public JobPayload getOnSuccessPayload() {
    return onSuccessPayload;
  }

  public void setOnSuccessPayload(JobPayload onSuccessPayload) {
    this.onSuccessPayload = onSuccessPayload;
  }

  public JobPayload getOnFailurePayload() {
    return onFailurePayload;
  }

  public void setOnFailurePayload(JobPayload onFailurePayload) {
    this.onFailurePayload = onFailurePayload;
  }

  public UUID getDependsOn() {
    return dependsOn;
  }

  public UUID getRecurringMasterId() {
    return recurringMasterId;
  }

  public void setRecurringMasterId(UUID recurringMasterId) {
    this.recurringMasterId = recurringMasterId;
  }

  public void setDependsOn(UUID dependsOn) {
    this.dependsOn = dependsOn;
  }

  public UUID getSupersededBy() {
    return supersededBy;
  }

  public void setSupersededBy(UUID supersededBy) {
    this.supersededBy = supersededBy;
  }

  public String getPickedBy() {
    return pickedBy;
  }

  public void setPickedBy(String pickedBy) {
    this.pickedBy = pickedBy;
  }

  public Instant getPickedAt() {
    return pickedAt;
  }

  public void setPickedAt(Instant pickedAt) {
    this.pickedAt = pickedAt;
  }

  public String getLastError() {
    return lastError;
  }

  public void setLastError(String lastError) {
    this.lastError = lastError;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public String getCallerPrincipal() {
    return callerPrincipal;
  }

  /**
   * Write-once for non-null values. Once a non-null caller principal has been recorded, attempts to
   * overwrite it with a different value (including null) are silently ignored. Idempotent re-set to
   * the same value is allowed. Pairs with {@code @Column(updatable = false)} on the field for
   * defense in depth: JPA blocks the column at the SQL layer; this blocks in-memory mutation.
   */
  public void setCallerPrincipal(String callerPrincipal) {
    if (this.callerPrincipal != null && !this.callerPrincipal.equals(callerPrincipal)) {
      return;
    }
    this.callerPrincipal = callerPrincipal;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }

  public Instant getExecutionStartTime() {
    return executionStartTime;
  }

  public void setExecutionStartTime(Instant executionStartTime) {
    this.executionStartTime = executionStartTime;
  }

  public Instant getExecutionEndTime() {
    return executionEndTime;
  }

  public void setExecutionEndTime(Instant executionEndTime) {
    this.executionEndTime = executionEndTime;
  }

  public Long getExecutionDurationMs() {
    return executionDurationMs;
  }

  public void setExecutionDurationMs(Long executionDurationMs) {
    this.executionDurationMs = executionDurationMs;
  }

  public Long getQueueWaitMs() {
    return queueWaitMs;
  }

  public void setQueueWaitMs(Long queueWaitMs) {
    this.queueWaitMs = queueWaitMs;
  }

  public String getJobResult() {
    return jobResult;
  }

  public void setJobResult(String jobResult) {
    this.jobResult = jobResult;
  }

  public String getResultType() {
    return resultType;
  }

  public void setResultType(String resultType) {
    this.resultType = resultType;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public String getSignalKey() {
    return signalKey;
  }

  public void setSignalKey(String signalKey) {
    this.signalKey = signalKey;
  }

  public Instant getSignalTimeout() {
    return signalTimeout;
  }

  public void setSignalTimeout(Instant signalTimeout) {
    this.signalTimeout = signalTimeout;
  }

  public String getSignalPayload() {
    return signalPayload;
  }

  public void setSignalPayload(String signalPayload) {
    this.signalPayload = signalPayload;
  }

  public String getSignalPayloadType() {
    return signalPayloadType;
  }

  public void setSignalPayloadType(String signalPayloadType) {
    this.signalPayloadType = signalPayloadType;
  }

  public String getSignalOutcome() {
    return signalOutcome;
  }

  public void setSignalOutcome(String signalOutcome) {
    this.signalOutcome = signalOutcome;
  }

  public String getSignalRejectionReason() {
    return signalRejectionReason;
  }

  public void setSignalRejectionReason(String signalRejectionReason) {
    this.signalRejectionReason = signalRejectionReason;
  }

  public Instant getSignalDeliveredAt() {
    return signalDeliveredAt;
  }

  public void setSignalDeliveredAt(Instant signalDeliveredAt) {
    this.signalDeliveredAt = signalDeliveredAt;
  }

  public String getSignalDeliveredBy() {
    return signalDeliveredBy;
  }

  public void setSignalDeliveredBy(String signalDeliveredBy) {
    this.signalDeliveredBy = signalDeliveredBy;
  }

  public String getSignalDeliveryId() {
    return signalDeliveryId;
  }

  public void setSignalDeliveryId(String signalDeliveryId) {
    this.signalDeliveryId = signalDeliveryId;
  }

  public JobStatus getTerminalStatus() {
    return terminalStatus;
  }

  public void setTerminalStatus(JobStatus terminalStatus) {
    this.terminalStatus = terminalStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobEntity jobEntity = (JobEntity) o;
    return Objects.equals(id, jobEntity.id);
  }

  @PrePersist
  void prePersist() {
    validateRequiredFields();
    Instant now = Instant.now();
    createdAt = now;
    updatedAt = now;
  }

  @PreUpdate
  void preUpdate() {
    validateRequiredFields();
    updatedAt = Instant.now();
  }

  private void validateRequiredFields() {
    requireRequiredField(status, "status");
    requireRequiredField(scheduledTime, "scheduledTime");
    requireRequiredField(jobType, "jobType");
    requireRequiredField(priority, "priority");
    requireRequiredField(backoffPolicy, "backoffPolicy");
    requireRequiredField(cronExpr, "cronExpr");
    requireRequiredField(zoneId, "zoneId");
    requireRequiredField(payload, "payload");
    requireRequiredField(idempotencyKey, "idempotencyKey");
  }

  private static void requireRequiredField(Object value, String fieldName) {
    if (value == null) {
      throw new IllegalStateException("JobEntity." + fieldName + " is required");
    }
  }
}
