package run.ratchet.store.postgresql;

import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
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

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return jobType == JobExecutionType.SINGLE
        || jobType == JobExecutionType.BATCH_CHILD
        || jobType == JobExecutionType.CHAIN_STEP
        || jobType == JobExecutionType.WORKFLOW_BRANCH;
  }

  static boolean isLiveStatus(JobStatus status) {
    return status == JobStatus.PENDING || status == JobStatus.RUNNING || status == JobStatus.PAUSED;
  }

  static boolean isTerminalStatus(JobStatus status) {
    return status == JobStatus.SUCCEEDED
        || status == JobStatus.FAILED
        || status == JobStatus.CANCELED;
  }

  static JobStatus effectiveStatus(JobStatus status) {
    return status == null ? JobStatus.PENDING : status;
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

  long countByNative(String sql, Object... params) {
    var query = em.createNativeQuery(sql);
    for (int i = 0; i < params.length; i++) {
      query.setParameter(i + 1, params[i]);
    }
    return ((Number) query.getSingleResult()).longValue();
  }
}
