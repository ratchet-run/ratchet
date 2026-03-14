package run.ratchet.store.entity;

import run.ratchet.store.id.TsidEntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Entity representing an active permit held by a job for a resource.
 *
 * <p>Permits are acquired before job execution and released when the job completes. The total
 * number of active permits for a resource is limited by {@link
 * ResourceLimitEntity#getMaxConcurrent()}.
 *
 * @see ResourceLimitEntity
 */
@Entity
@Table(
    name = "scheduler_resource_permit",
    indexes = {
      @Index(name = "idx_resource_permit_resource", columnList = "resource_name"),
      @Index(name = "idx_resource_permit_job", columnList = "job_id")
    })
@EntityListeners(TsidEntityListener.class)
public class ResourcePermitEntity implements TsidEntityListener.TsidAssignable {

  @Id private Long id;

  @Column(name = "resource_name", nullable = false, length = 100)
  private String resourceName;

  @Column(name = "job_id", nullable = false)
  private Long jobId;

  @Column(name = "node_id", nullable = false, length = 64)
  private String nodeId;

  @Column(name = "acquired_at", nullable = false)
  private Instant acquiredAt;

  // ── Getters ──────────────────────────────────────────────────────────────

  /**
   * Creates a new permit for a job to access a resource.
   *
   * @param resourceName the resource being accessed
   * @param jobId the job requesting access
   * @param nodeId the node executing the job
   * @return a new permit entity ready to be persisted
   */
  public static ResourcePermitEntity create(String resourceName, Long jobId, String nodeId) {
    ResourcePermitEntity entity = new ResourcePermitEntity();
    entity.setResourceName(resourceName);
    entity.setJobId(jobId);
    entity.setNodeId(nodeId);
    entity.setAcquiredAt(Instant.now());
    return entity;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  // ── Setters ──────────────────────────────────────────────────────────────

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public Instant getAcquiredAt() {
    return acquiredAt;
  }

  // ── Factory methods ─────────────────────────────────────────────────────

  public void setAcquiredAt(Instant acquiredAt) {
    this.acquiredAt = acquiredAt;
  }
}
