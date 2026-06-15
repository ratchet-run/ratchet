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
package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Performance metrics for batch job executions.
 *
 * @see BatchEntity for progress tracking
 * @see JobEntity for the parent batch job
 */
@Entity
@Table(name = "scheduler_batch_metrics")
public class BatchMetricsEntity {

  @Id
  @Column(name = "batch_id")
  private UUID batchId;

  // lazy fetch to avoid eagerly joining scheduler_job for columns that no longer exist on
  // cold post hot/cold-split (status, attempts, picked_*, scheduled_time, updated_at, version,
  // last_error, paused_from_status). The batchJob field is link-only and never traversed by
  // application code.
  @OneToOne(fetch = FetchType.LAZY, optional = false)
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

  public BatchMetricsEntity() {}

  public BatchMetricsEntity(
      UUID batchId,
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

  public UUID getBatchId() {
    return batchId;
  }

  public void setBatchId(UUID batchId) {
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

  public void setVersion(Integer version) {
    this.version = version;
  }

  /** Overhead as a percentage of total execution time, or 0 if not yet calculated. */
  public double getOverheadPercent() {
    if (totalDurationMs == null || totalDurationMs == 0) {
      return 0;
    }
    if (overheadMs == null) {
      return 0;
    }
    return (overheadMs / (double) totalDurationMs) * 100;
  }

  /** Success rate of child jobs as a percentage (0-100). */
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
    return Objects.equals(batchId, that.batchId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(batchId);
  }
}
