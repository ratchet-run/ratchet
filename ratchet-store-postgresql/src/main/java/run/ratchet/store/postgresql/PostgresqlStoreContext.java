package run.ratchet.store.postgresql;

import run.ratchet.api.exception.RatchetTransientStoreException;
import jakarta.persistence.EntityManager;

final class PostgresqlStoreContext {

  private final EntityManager em;
  private final int priorityBoostIntervalMinutes;
  private final PostgresqlConstraintDetector constraintDetector =
      new PostgresqlConstraintDetector();

  PostgresqlStoreContext(EntityManager em) {
    this(em, 15);
  }

  PostgresqlStoreContext(EntityManager em, int priorityBoostIntervalMinutes) {
    this.em = em;
    this.priorityBoostIntervalMinutes = priorityBoostIntervalMinutes;
  }

  EntityManager em() {
    return em;
  }

  PostgresqlConstraintDetector constraintDetector() {
    return constraintDetector;
  }

  int priorityBoostIntervalMinutes() {
    return priorityBoostIntervalMinutes;
  }

  RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    if (constraintDetector.isDeadlock(e) || constraintDetector.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient PostgreSQL store concurrency failure during " + operation, e);
    }
    return e;
  }
}
