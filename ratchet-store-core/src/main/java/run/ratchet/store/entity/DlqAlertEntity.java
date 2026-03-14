package run.ratchet.store.entity;

import run.ratchet.store.id.TsidEntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Instant;
import java.util.Objects;

/**
 * Entity representing a record of a DLQ (Dead Letter Queue) alert notification.
 *
 * <p>Each record tracks an alert sent for a permanently failed job, enabling rate-limiting of
 * duplicate alerts and providing an audit trail of notifications.
 */
@Entity
@Table(
    name = "scheduler_dlq_alerts",
    uniqueConstraints =
        @UniqueConstraint(
            name = "uk_job_error_hash",
            columnNames = {"job_id", "error_hash"}),
    indexes = @Index(name = "idx_dlq_sent_at", columnList = "alert_sent_at"))
@EntityListeners(TsidEntityListener.class)
public class DlqAlertEntity implements TsidEntityListener.TsidAssignable {

  @Id private Long id;

  @Column(name = "job_id", nullable = false)
  private Long jobId;

  @Column(name = "error_hash", nullable = false, length = 64)
  private String errorHash;

  @Column(name = "alert_sent_at")
  private Instant alertSentAt;

  @Column(name = "alert_channel", length = 100)
  private String alertChannel;

  // ── Getters ──────────────────────────────────────────────────────────────

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

  public String getErrorHash() {
    return errorHash;
  }

  // ── Setters ──────────────────────────────────────────────────────────────

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

  // ── Object overrides ────────────────────────────────────────────────────

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
