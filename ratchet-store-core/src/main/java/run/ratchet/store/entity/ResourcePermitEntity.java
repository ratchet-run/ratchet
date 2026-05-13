package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.store.id.UuidV7EntityListener;

/** Active resource permit (JPA entity). */
@Entity
@Table(
    name = "scheduler_resource_permit",
    indexes = {
      @Index(name = "idx_resource_permit_resource", columnList = "resource_name"),
      @Index(name = "idx_resource_permit_job", columnList = "job_id")
    })
@EntityListeners(UuidV7EntityListener.class)
public class ResourcePermitEntity implements UuidV7EntityListener.UuidV7Assignable {

  @Id private UUID id;

  @Column(name = "resource_name", nullable = false, length = 100)
  private String resourceName;

  @Column(name = "job_id", nullable = false)
  private UUID jobId;

  @Column(name = "node_id", nullable = false, length = 64)
  private String nodeId;

  @Column(name = "acquired_at", nullable = false)
  private Instant acquiredAt;

  public static ResourcePermitEntity create(String resourceName, UUID jobId, String nodeId) {
    ResourcePermitEntity entity = new ResourcePermitEntity();
    entity.setResourceName(resourceName);
    entity.setJobId(jobId);
    entity.setNodeId(nodeId);
    entity.setAcquiredAt(Instant.now());
    return entity;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public UUID getJobId() {
    return jobId;
  }

  public void setJobId(UUID jobId) {
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

  public void setAcquiredAt(Instant acquiredAt) {
    this.acquiredAt = acquiredAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourcePermitEntity that = (ResourcePermitEntity) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }
}
