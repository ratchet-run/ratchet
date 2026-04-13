package run.ratchet.store.id;

import jakarta.persistence.PrePersist;

/** Assigns TSID on @PrePersist if not already set. */
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
