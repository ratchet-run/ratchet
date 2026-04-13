package run.ratchet.store.entity;

import run.ratchet.store.id.TsidEntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Single execution attempt of a job, providing an audit trail for debugging and performance
 * analysis.
 *
 * @see JobEntity for the parent job
 */
@Entity
@Table(
    name = "scheduler_job_execution",
    indexes = {
      @Index(name = "idx_job_execution_job", columnList = "job_id"),
      @Index(name = "idx_job_execution_node", columnList = "node_id, started_at"),
      @Index(name = "idx_job_execution_status", columnList = "status, started_at")
    })
@EntityListeners(TsidEntityListener.class)
public class JobExecutionEntity implements TsidEntityListener.TsidAssignable {

  @Id private Long id;

  @Column(name = "job_id", nullable = false)
  private Long jobId;

  @Column(name = "attempt", nullable = false)
  private int attempt;

  @Column(name = "node_id", nullable = false, length = 64)
  private String nodeId;

  @Column(name = "started_at", nullable = false)
  private Instant startedAt;

  @Column(name = "ended_at")
  private Instant endedAt;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private ExecutionStatus status = ExecutionStatus.RUNNING;

  @Column(name = "error_message", columnDefinition = "TEXT")
  private String errorMessage;

  @Column(name = "error_class")
  private String errorClass;

  @Column(name = "duration_ms")
  private Long durationMs;

  public static JobExecutionEntity start(Long jobId, int attempt, String nodeId) {
    JobExecutionEntity entity = new JobExecutionEntity();
    entity.setJobId(jobId);
    entity.setAttempt(attempt);
    entity.setNodeId(nodeId);
    entity.setStartedAt(Instant.now());
    entity.setStatus(ExecutionStatus.RUNNING);
    return entity;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  public int getAttempt() {
    return attempt;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public Instant getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(Instant startedAt) {
    this.startedAt = startedAt;
  }

  public Instant getEndedAt() {
    return endedAt;
  }

  public void setEndedAt(Instant endedAt) {
    this.endedAt = endedAt;
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  public void setStatus(ExecutionStatus status) {
    this.status = status;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public String getErrorClass() {
    return errorClass;
  }

  public void setErrorClass(String errorClass) {
    this.errorClass = errorClass;
  }

  public Long getDurationMs() {
    return durationMs;
  }

  public void setDurationMs(Long durationMs) {
    this.durationMs = durationMs;
  }

  public void markSucceeded() {
    this.endedAt = Instant.now();
    this.status = ExecutionStatus.SUCCEEDED;
    this.durationMs = endedAt.toEpochMilli() - startedAt.toEpochMilli();
  }

  public void markFailed(Throwable exception) {
    this.endedAt = Instant.now();
    this.status = ExecutionStatus.FAILED;
    this.durationMs = endedAt.toEpochMilli() - startedAt.toEpochMilli();
    if (exception != null) {
      this.errorClass = exception.getClass().getName();
      String message = exception.getMessage();
      if (message != null && message.length() > 65535) {
        message = message.substring(0, 65532) + "...";
      }
      this.errorMessage = message;
    }
  }

  public void markCanceled() {
    this.endedAt = Instant.now();
    this.status = ExecutionStatus.CANCELED;
    this.durationMs = endedAt.toEpochMilli() - startedAt.toEpochMilli();
    this.errorMessage = "Job was canceled during execution - result discarded";
  }

  public enum ExecutionStatus {
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELED
  }
}
