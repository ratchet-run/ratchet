package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.store.converter.JsonObjectMapConverter;
import run.ratchet.store.id.UuidV7EntityListener;

/** Immutable log entry for job execution events. */
@Entity
@Table(
    name = "scheduler_job_log",
    indexes = {
      @Index(name = "idx_joblog_job_ts", columnList = "job_id, ts"),
      @Index(name = "idx_joblog_ts", columnList = "ts")
    })
@EntityListeners(UuidV7EntityListener.class)
public class JobLogEntity implements UuidV7EntityListener.UuidV7Assignable {

  @Id
  @Column(name = "log_id")
  private UUID id;

  @Column(name = "job_id", nullable = false)
  private UUID jobId;

  @Column(name = "ts", nullable = false)
  private Instant ts;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 8)
  private LogLevel level;

  @Column(nullable = false)
  private String message;

  @Convert(converter = JsonObjectMapConverter.class)
  private Map<String, Object> mdc;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getJobId() {
    return jobId;
  }

  public void setJobId(UUID jobId) {
    this.jobId = jobId;
  }

  public Instant getTs() {
    return ts;
  }

  public void setTs(Instant ts) {
    this.ts = ts;
  }

  public LogLevel getLevel() {
    return level;
  }

  public void setLevel(LogLevel level) {
    this.level = level;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Map<String, Object> getMdc() {
    return mdc;
  }

  public void setMdc(Map<String, Object> mdc) {
    this.mdc = mdc;
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, ts, level, message);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobLogEntity that = (JobLogEntity) o;
    return Objects.equals(jobId, that.jobId)
        && Objects.equals(ts, that.ts)
        && Objects.equals(level, that.level)
        && Objects.equals(message, that.message);
  }

  /** Log severity levels for job execution events, ordered from least to most severe. */
  public enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
  }
}
