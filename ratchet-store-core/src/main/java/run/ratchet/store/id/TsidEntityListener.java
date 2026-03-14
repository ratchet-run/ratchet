package run.ratchet.store.id;

import jakarta.persistence.PrePersist;

/**
 * JPA entity listener that assigns a TSID to entities before they are persisted.
 *
 * <p>Works with any entity that has a {@code Long getId()} / {@code void setId(Long)} pair. If the
 * ID is already set (non-null and non-zero), it is left unchanged — this allows pre-assigned IDs
 * for imports or migrations.
 */
public class TsidEntityListener {

  @PrePersist
  public void assignTsid(Object entity) {
    if (entity instanceof TsidAssignable assignable) {
      if (assignable.getId() == null || assignable.getId() == 0L) {
        assignable.setId(TsidFactory.next());
      }
    }
  }

  /**
   * Marker interface for entities that receive TSID assignment. Implemented by all entities that
   * previously used {@code @GeneratedValue(strategy = IDENTITY)}.
   */
  public interface TsidAssignable {

    Long getId();

    void setId(Long id);
  }
}
