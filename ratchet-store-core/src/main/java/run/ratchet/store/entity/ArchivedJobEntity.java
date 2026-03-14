package run.ratchet.store.entity;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.id.TsidEntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Objects;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Represents an archived job entity for completed, failed, or canceled jobs.
 *
 * <p>This entity stores historical job data for audit trails, analytics, and compliance purposes.
 * Jobs are moved to this archive table after reaching a terminal state and after a configurable
 * retention period in the active jobs table.
 *
 * @see JobEntity for the active job entity
 * @see JobStatus for possible final status values
 */
@Entity
@Table(
    name = "scheduler_job_archive",
    indexes = {
      @Index(name = "idx_archive_original_id", columnList = "original_job_id"),
      @Index(name = "idx_archive_status", columnList = "final_status"),
      @Index(name = "idx_archive_created_range", columnList = "original_created_at"),
      @Index(name = "idx_archive_completed_range", columnList = "completion_time"),
      @Index(name = "idx_archive_archived_at", columnList = "archived_at"),
      @Index(name = "idx_archive_target_class", columnList = "target_class"),
      @Index(name = "idx_archive_business_key", columnList = "business_key"),
      @Index(name = "idx_archive_job_type", columnList = "job_type"),
      @Index(name = "idx_archive_priority", columnList = "priority")
    })
@EntityListeners(TsidEntityListener.class)
public class ArchivedJobEntity implements TsidEntityListener.TsidAssignable {

  @Id
  @Column(name = "archive_id")
  private Long id;

  @Column(name = "original_job_id", nullable = false)
  private Long originalJobId;

  @Enumerated(EnumType.STRING)
  @Column(name = "final_status", nullable = false, length = 16)
  private JobStatus finalStatus;

  @Enumerated(EnumType.STRING)
  @Column(name = "job_type", nullable = false, length = 16)
  private JobExecutionType jobType;

  @Enumerated(EnumType.ORDINAL)
  @Column(nullable = false)
  private JobPriority priority;

  @Column(name = "total_attempts", nullable = false)
  private int totalAttempts;

  @Column(name = "max_retries", nullable = false)
  private int maxRetries;

  @Enumerated(EnumType.STRING)
  @Column(name = "backoff_policy", nullable = false, length = 16)
  private BackoffPolicy backoffPolicy;

  @Column(name = "backoff_param_ms", nullable = false)
  private int backoffParamMs;

  @Column(name = "timeout_sec", nullable = false)
  private int timeoutSec;

  @Column(name = "target_class")
  private String targetClass;

  @Column(name = "method_name", length = 128)
  private String methodName;

  @Column(name = "business_key")
  private String businessKey;

  @Column(name = "cron_expr", length = 64)
  private String cronExpr;

  @Column(name = "zone_id", length = 32)
  private String zoneId;

  @Column(name = "original_scheduled_time", nullable = false)
  private Instant originalScheduledTime;

  @Column(name = "original_created_at", nullable = false)
  private Instant originalCreatedAt;

  @Column(name = "first_execution_time")
  private Instant firstExecutionTime;

  @Column(name = "completion_time")
  private Instant completionTime;

  @Column(name = "total_execution_time_ms")
  private Long totalExecutionTimeMs;

  @Column(name = "queue_wait_ms")
  private Long queueWaitMs;

  @Column(name = "archived_at", nullable = false)
  private Instant archivedAt;

  @Column(name = "archived_by", length = 64)
  private String archivedBy;

  @Column(name = "archive_reason", length = 128)
  private String archiveReason;

  @Column(name = "job_result")
  @JdbcTypeCode(SqlTypes.JSON)
  private String jobResult;

  @Column(name = "result_type", length = 100)
  private String resultType;

  @Column(name = "final_error")
  private String finalError;

  @Column(name = "payload_summary")
  private String payloadSummary;

  @Column(name = "depended_on")
  private Long dependedOn;

  @Column(name = "superseded_by")
  private Long supersededBy;

  @Column(name = "tags", length = 512)
  private String tags;

  // ── Getters ──────────────────────────────────────────────────────────────

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getOriginalJobId() {
    return originalJobId;
  }

  public void setOriginalJobId(Long originalJobId) {
    this.originalJobId = originalJobId;
  }

  public JobStatus getFinalStatus() {
    return finalStatus;
  }

  public void setFinalStatus(JobStatus finalStatus) {
    this.finalStatus = finalStatus;
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

  public int getTotalAttempts() {
    return totalAttempts;
  }

  public void setTotalAttempts(int totalAttempts) {
    this.totalAttempts = totalAttempts;
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

  public String getBusinessKey() {
    return businessKey;
  }

  public void setBusinessKey(String businessKey) {
    this.businessKey = businessKey;
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

  public Instant getOriginalScheduledTime() {
    return originalScheduledTime;
  }

  // ── Setters ──────────────────────────────────────────────────────────────

  public void setOriginalScheduledTime(Instant originalScheduledTime) {
    this.originalScheduledTime = originalScheduledTime;
  }

  public Instant getOriginalCreatedAt() {
    return originalCreatedAt;
  }

  public void setOriginalCreatedAt(Instant originalCreatedAt) {
    this.originalCreatedAt = originalCreatedAt;
  }

  public Instant getFirstExecutionTime() {
    return firstExecutionTime;
  }

  public void setFirstExecutionTime(Instant firstExecutionTime) {
    this.firstExecutionTime = firstExecutionTime;
  }

  public Instant getCompletionTime() {
    return completionTime;
  }

  public void setCompletionTime(Instant completionTime) {
    this.completionTime = completionTime;
  }

  public Long getTotalExecutionTimeMs() {
    return totalExecutionTimeMs;
  }

  public void setTotalExecutionTimeMs(Long totalExecutionTimeMs) {
    this.totalExecutionTimeMs = totalExecutionTimeMs;
  }

  public Long getQueueWaitMs() {
    return queueWaitMs;
  }

  public void setQueueWaitMs(Long queueWaitMs) {
    this.queueWaitMs = queueWaitMs;
  }

  public Instant getArchivedAt() {
    return archivedAt;
  }

  public void setArchivedAt(Instant archivedAt) {
    this.archivedAt = archivedAt;
  }

  public String getArchivedBy() {
    return archivedBy;
  }

  public void setArchivedBy(String archivedBy) {
    this.archivedBy = archivedBy;
  }

  public String getArchiveReason() {
    return archiveReason;
  }

  public void setArchiveReason(String archiveReason) {
    this.archiveReason = archiveReason;
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

  public String getFinalError() {
    return finalError;
  }

  public void setFinalError(String finalError) {
    this.finalError = finalError;
  }

  public String getPayloadSummary() {
    return payloadSummary;
  }

  public void setPayloadSummary(String payloadSummary) {
    this.payloadSummary = payloadSummary;
  }

  public Long getDependedOn() {
    return dependedOn;
  }

  public void setDependedOn(Long dependedOn) {
    this.dependedOn = dependedOn;
  }

  public Long getSupersededBy() {
    return supersededBy;
  }

  public void setSupersededBy(Long supersededBy) {
    this.supersededBy = supersededBy;
  }

  public String getTags() {
    return tags;
  }

  public void setTags(String tags) {
    this.tags = tags;
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
    ArchivedJobEntity that = (ArchivedJobEntity) o;
    return Objects.equals(id, that.id);
  }

  @PrePersist
  void prePersist() {
    if (archivedAt == null) {
      archivedAt = Instant.now();
    }
  }
}
