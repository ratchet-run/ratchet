package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;

/** Distributed lock for cluster-wide synchronization, with automatic expiration. */
@Entity
@Table(
    name = "scheduler_lock",
    indexes = @Index(name = "idx_lock_expires", columnList = "expires_at"))
public class LockEntity {

  @Id
  @Column(name = "lock_name", length = 128)
  private String lockName;

  @Column(name = "owner_node", length = 64, nullable = false)
  private String ownerNode;

  @Column(name = "locked_at", nullable = false)
  private Instant lockedAt;

  @Column(name = "expires_at", nullable = false)
  private Instant expiresAt;

  public String getLockName() {
    return lockName;
  }

  public void setLockName(String lockName) {
    this.lockName = lockName;
  }

  public String getOwnerNode() {
    return ownerNode;
  }

  public void setOwnerNode(String ownerNode) {
    this.ownerNode = ownerNode;
  }

  public Instant getLockedAt() {
    return lockedAt;
  }

  public void setLockedAt(Instant lockedAt) {
    this.lockedAt = lockedAt;
  }

  public Instant getExpiresAt() {
    return expiresAt;
  }

  public void setExpiresAt(Instant expiresAt) {
    this.expiresAt = expiresAt;
  }
}
