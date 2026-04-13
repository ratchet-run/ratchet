package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Configured resource concurrency limit.
 *
 * @see ResourcePermitEntity
 */
@Entity
@Table(name = "scheduler_resource_limit")
public class ResourceLimitEntity {

  @Id
  @Column(name = "resource_name", nullable = false, length = 100)
  private String resourceName;

  @Column(name = "max_concurrent", nullable = false)
  private int maxConcurrent;

  @Column(name = "retry_delay_ms", nullable = false)
  private int retryDelayMs = 5000;

  @Column(name = "description")
  private String description;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public int getMaxConcurrent() {
    return maxConcurrent;
  }

  public void setMaxConcurrent(int maxConcurrent) {
    this.maxConcurrent = maxConcurrent;
  }

  public int getRetryDelayMs() {
    return retryDelayMs;
  }

  public void setRetryDelayMs(int retryDelayMs) {
    this.retryDelayMs = retryDelayMs;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }
}
