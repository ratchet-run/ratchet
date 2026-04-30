package run.ratchet.store.entity;

import run.ratchet.store.id.UuidV7EntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** DLQ alert notification record, tracking alerts sent for permanently failed jobs. */
@Entity
@Table(
    name = "scheduler_dlq_alerts",
    uniqueConstraints =
        @UniqueConstraint(
            name = "uk_job_error_hash",
            columnNames = {"job_id", "error_hash"}),
    indexes = @Index(name = "idx_dlq_sent_at", columnList = "alert_sent_at"))
@EntityListeners(UuidV7EntityListener.class)
public class DlqAlertEntity implements UuidV7EntityListener.UuidV7Assignable {

  @Id private UUID id;

  @Column(name = "job_id", nullable = false)
  private UUID jobId;

  @Column(name = "error_hash", nullable = false, length = 64)
  private String errorHash;

  @Column(name = "alert_sent_at")
  private Instant alertSentAt;

  @Column(name = "alert_channel", length = 100)
  private String alertChannel;

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

  public String getErrorHash() {
    return errorHash;
  }

  public void setErrorHash(String errorHash) {
    this.errorHash = errorHash;
  }

  public Instant getAlertSentAt() {
    return alertSentAt;
  }

  public void setAlertSentAt(Instant alertSentAt) {
    this.alertSentAt = alertSentAt;
  }

  public String getAlertChannel() {
    return alertChannel;
  }

  public void setAlertChannel(String alertChannel) {
    this.alertChannel = alertChannel;
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
    DlqAlertEntity that = (DlqAlertEntity) o;
    return Objects.equals(id, that.id);
  }
}
