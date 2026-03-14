package run.ratchet.store.entity;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.id.TsidEntityListener;
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
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Core entity representing a unit of work in the distributed job scheduler.
 *
 * <p>JobEntity is the central persistence model for all scheduled tasks, containing scheduling
 * metadata, execution state, retry configuration, and business logic references.
 *
 * <h2>Job Lifecycle:</h2>
 *
 * <ol>
 *   <li><b>Creation:</b> Job scheduled with target time and configuration
 *   <li><b>Polling:</b> Becomes visible when scheduled_time &lt;= now and status = PENDING
 *   <li><b>Execution:</b> Worker picks up job, sets RUNNING status with ownership
 *   <li><b>Completion:</b> Success/failure recorded with metrics and results
 *   <li><b>Archival:</b> Moved to archive table after retention period
 * </ol>
 *
 * @see JobStatus for state machine details
 * @see JobExecutionType for internal execution roles
 */
@Entity
@Table(
    name = "scheduler_job",
    indexes = {
      @Index(name = "idx_job_due", columnList = "status, scheduled_time"),
      @Index(name = "idx_job_priority_due", columnList = "priority, scheduled_time"),
      @Index(name = "idx_job_picked_by", columnList = "picked_by"),
      @Index(name = "idx_target_class", columnList = "target_class"),
      @Index(name = "idx_method_name", columnList = "method_name"),
      @Index(name = "idx_recurring_due", columnList = "status, next_fire"),
      @Index(name = "idx_job_poll_composite", columnList = "status, priority, scheduled_time"),
      @Index(name = "idx_job_type", columnList = "job_type"),
      @Index(name = "idx_job_recurring_composite", columnList = "job_type, status, next_fire"),
      @Index(name = "idx_job_depends_on", columnList = "depends_on"),
      @Index(name = "idx_job_superseded_by", columnList = "superseded_by"),
      @Index(name = "idx_job_business_key", columnList = "business_key"),
      @Index(name = "idx_job_created_at", columnList = "created_at"),
      @Index(name = "idx_job_updated_at", columnList = "updated_at")
    })
@EntityListeners(TsidEntityListener.class)
public class JobEntity implements TsidEntityListener.TsidAssignable {

  @Id
  @Column(name = "job_id")
  private Long id;

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

  @Column(name = "next_fire")
  private Instant nextFire;

  @Convert(converter = JobPayloadConverter.class)
  @Column(nullable = false)
  @JdbcTypeCode(SqlTypes.JSON)
  private JobPayload payload;

  @Convert(converter = JsonMapConverter.class)
  @JdbcTypeCode(SqlTypes.JSON)
  private Map<String, String> params;

  @Column(name = "target_class", insertable = false, updatable = false)
  private String targetClass;

  @Column(name = "method_name", insertable = false, updatable = false)
  private String methodName;

  @Column(name = "idempotency_key", nullable = false, unique = true, length = 36)
  private String idempotencyKey;

  @Column(name = "business_key")
  private String businessKey;

  @ElementCollection(fetch = FetchType.LAZY)
  @CollectionTable(name = "scheduler_job_tag", joinColumns = @JoinColumn(name = "job_id"))
  @Column(name = "tag", length = 64)
  private List<String> tags;

  @Column(name = "resource_name", length = 100)
  private String resourceName;

  @Column(name = "depends_on")
  private Long dependsOn;

  @Column(name = "superseded_by")
  private Long supersededBy;

  @Column(name = "picked_by", length = 64)
  private String pickedBy;

  @Column(name = "picked_at")
  private Instant pickedAt;

  @Column(name = "last_error")
  private String lastError;

  @Column(name = "created_at", updatable = false)
  private Instant createdAt;

  @Column(name = "created_by", updatable = false)
  private String createdBy;

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
  @JdbcTypeCode(SqlTypes.JSON)
  private String jobResult;

  @Column(name = "result_type", length = 100)
  private String resultType;

  @Version
  @Column(name = "version")
  private Integer version;

  // ── Getters ──────────────────────────────────────────────────────────────

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
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

  // ── Setters ──────────────────────────────────────────────────────────────

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

  public Long getDependsOn() {
    return dependsOn;
  }

  public void setDependsOn(Long dependsOn) {
    this.dependsOn = dependsOn;
  }

  public Long getSupersededBy() {
    return supersededBy;
  }

  public void setSupersededBy(Long supersededBy) {
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

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
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

  // ── Object overrides & lifecycle ─────────────────────────────────────────

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
    Instant now = Instant.now();
    createdAt = now;
    updatedAt = now;
  }

  @PreUpdate
  void preUpdate() {
    updatedAt = Instant.now();
  }
}
