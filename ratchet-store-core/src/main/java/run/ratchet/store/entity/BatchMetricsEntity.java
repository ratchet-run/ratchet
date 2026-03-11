package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.Objects;

/**
 * Entity for tracking detailed performance metrics of batch job executions.
 *
 * <p>This entity captures comprehensive timing and success rate metrics for batch jobs.
 *
 * @see BatchEntity for progress tracking
 * @see JobEntity for the parent batch job
 */
@Entity
@Table(name = "scheduler_batch_metrics")
public class BatchMetricsEntity {

  @Id
  @Column(name = "batch_id")
  private Long batchId;

  @OneToOne
  @MapsId
  @JoinColumn(name = "batch_id")
  private JobEntity batchJob;

  @Column(name = "total_duration_ms")
  private Long totalDurationMs;

  @Column(name = "child_execution_ms")
  private Long childExecutionMs;

  @Column(name = "overhead_ms")
  private Long overheadMs;

  @Column(name = "child_count", nullable = false)
  private int childCount = 0;

  @Column(name = "success_count", nullable = false)
  private int successCount = 0;

  @Column(name = "failure_count", nullable = false)
  private int failureCount = 0;

  @Column(name = "started_at")
  private Instant startedAt;

  @Column(name = "completed_at")
  private Instant completedAt;

  @Version
  @Column(name = "version")
  private Integer version;

  // ── Constructors ─────────────────────────────────────────────────────────

  public BatchMetricsEntity() {}

  public BatchMetricsEntity(
      Long batchId,
      JobEntity batchJob,
      Long totalDurationMs,
      Long childExecutionMs,
      Long overheadMs,
      int childCount,
      int successCount,
      int failureCount,
      Instant startedAt,
      Instant completedAt,
      Integer version) {
    this.batchId = batchId;
    this.batchJob = batchJob;
    this.totalDurationMs = totalDurationMs;
    this.childExecutionMs = childExecutionMs;
    this.overheadMs = overheadMs;
    this.childCount = childCount;
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.startedAt = startedAt;
    this.completedAt = completedAt;
    this.version = version;
  }

  // ── Getters ──────────────────────────────────────────────────────────────

  public static BatchMetricsEntityBuilder builder() {
    return new BatchMetricsEntityBuilder();
  }

  public Long getBatchId() {
    return batchId;
  }

  public void setBatchId(Long batchId) {
    this.batchId = batchId;
  }

  public JobEntity getBatchJob() {
    return batchJob;
  }

  public void setBatchJob(JobEntity batchJob) {
    this.batchJob = batchJob;
  }

  public Long getTotalDurationMs() {
    return totalDurationMs;
  }

  public void setTotalDurationMs(Long totalDurationMs) {
    this.totalDurationMs = totalDurationMs;
  }

  public Long getChildExecutionMs() {
    return childExecutionMs;
  }

  public void setChildExecutionMs(Long childExecutionMs) {
    this.childExecutionMs = childExecutionMs;
  }

  public Long getOverheadMs() {
    return overheadMs;
  }

  public void setOverheadMs(Long overheadMs) {
    this.overheadMs = overheadMs;
  }

  // ── Setters ──────────────────────────────────────────────────────────────

  public int getChildCount() {
    return childCount;
  }

  public void setChildCount(int childCount) {
    this.childCount = childCount;
  }

  public int getSuccessCount() {
    return successCount;
  }

  public void setSuccessCount(int successCount) {
    this.successCount = successCount;
  }

  public int getFailureCount() {
    return failureCount;
  }

  public void setFailureCount(int failureCount) {
    this.failureCount = failureCount;
  }

  public Instant getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(Instant startedAt) {
    this.startedAt = startedAt;
  }

  public Instant getCompletedAt() {
    return completedAt;
  }

  public void setCompletedAt(Instant completedAt) {
    this.completedAt = completedAt;
  }

  public Integer getVersion() {
    return version;
  }

  // ── Business methods ────────────────────────────────────────────────────

  public void setVersion(Integer version) {
    this.version = version;
  }

  /**
   * Calculates overhead as a percentage of total execution time.
   *
   * @return overhead percentage (0-100), or 0 if metrics are not yet calculated
   */
  public double getOverheadPercent() {
    if (totalDurationMs == null || totalDurationMs == 0) {
      return 0;
    }
    if (overheadMs == null) {
      return 0;
    }
    return (overheadMs / (double) totalDurationMs) * 100;
  }

  // ── Object overrides ────────────────────────────────────────────────────

  /**
   * Calculates the success rate of child jobs as a percentage.
   *
   * @return success rate percentage (0-100), or 0 if no child jobs have been processed
   */
  public double getSuccessRate() {
    if (childCount == 0) {
      return 0;
    }
    return (successCount / (double) childCount) * 100;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BatchMetricsEntity that = (BatchMetricsEntity) o;
    return childCount == that.childCount
        && successCount == that.successCount
        && failureCount == that.failureCount
        && Objects.equals(batchId, that.batchId)
        && Objects.equals(totalDurationMs, that.totalDurationMs)
        && Objects.equals(childExecutionMs, that.childExecutionMs)
        && Objects.equals(overheadMs, that.overheadMs)
        && Objects.equals(startedAt, that.startedAt)
        && Objects.equals(completedAt, that.completedAt)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        batchId,
        totalDurationMs,
        childExecutionMs,
        overheadMs,
        childCount,
        successCount,
        failureCount,
        startedAt,
        completedAt,
        version);
  }

  // ── Builder ─────────────────────────────────────────────────────────────

  @Override
  public String toString() {
    return "BatchMetricsEntity("
        + "batchId="
        + batchId
        + ", totalDurationMs="
        + totalDurationMs
        + ", childExecutionMs="
        + childExecutionMs
        + ", overheadMs="
        + overheadMs
        + ", childCount="
        + childCount
        + ", successCount="
        + successCount
        + ", failureCount="
        + failureCount
        + ", startedAt="
        + startedAt
        + ", completedAt="
        + completedAt
        + ", version="
        + version
        + ')';
  }

  public static class BatchMetricsEntityBuilder {

    private Long batchId;
    private JobEntity batchJob;
    private Long totalDurationMs;
    private Long childExecutionMs;
    private Long overheadMs;
    private int childCount;
    private int successCount;
    private int failureCount;
    private Instant startedAt;
    private Instant completedAt;
    private Integer version;

    BatchMetricsEntityBuilder() {}

    public BatchMetricsEntityBuilder batchId(Long batchId) {
      this.batchId = batchId;
      return this;
    }

    public BatchMetricsEntityBuilder batchJob(JobEntity batchJob) {
      this.batchJob = batchJob;
      return this;
    }

    public BatchMetricsEntityBuilder totalDurationMs(Long totalDurationMs) {
      this.totalDurationMs = totalDurationMs;
      return this;
    }

    public BatchMetricsEntityBuilder childExecutionMs(Long childExecutionMs) {
      this.childExecutionMs = childExecutionMs;
      return this;
    }

    public BatchMetricsEntityBuilder overheadMs(Long overheadMs) {
      this.overheadMs = overheadMs;
      return this;
    }

    public BatchMetricsEntityBuilder childCount(int childCount) {
      this.childCount = childCount;
      return this;
    }

    public BatchMetricsEntityBuilder successCount(int successCount) {
      this.successCount = successCount;
      return this;
    }

    public BatchMetricsEntityBuilder failureCount(int failureCount) {
      this.failureCount = failureCount;
      return this;
    }

    public BatchMetricsEntityBuilder startedAt(Instant startedAt) {
      this.startedAt = startedAt;
      return this;
    }

    public BatchMetricsEntityBuilder completedAt(Instant completedAt) {
      this.completedAt = completedAt;
      return this;
    }

    public BatchMetricsEntityBuilder version(Integer version) {
      this.version = version;
      return this;
    }

    public BatchMetricsEntity build() {
      return new BatchMetricsEntity(
          batchId,
          batchJob,
          totalDurationMs,
          childExecutionMs,
          overheadMs,
          childCount,
          successCount,
          failureCount,
          startedAt,
          completedAt,
          version);
    }
  }
}
