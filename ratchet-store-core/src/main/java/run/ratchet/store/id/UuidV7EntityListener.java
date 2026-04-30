package run.ratchet.store.id;

import jakarta.persistence.PrePersist;
import java.util.UUID;

/** Assigns a UUIDv7 on {@code @PrePersist} if the entity's id field is unset. */
public class UuidV7EntityListener {

  @PrePersist
  public void assignId(Object entity) {
    if (entity instanceof UuidV7Assignable assignable && assignable.getId() == null) {
      assignable.setId(UuidV7Factory.create());
    }
  }

  /**
   * Marker interface for entities that receive UUIDv7 assignment. Entities opt in by implementing
   * this interface and declaring {@code @EntityListeners(UuidV7EntityListener.class)}.
   */
  public interface UuidV7Assignable {

    UUID getId();

    void setId(UUID id);
  }
}
