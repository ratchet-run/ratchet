package run.ratchet.store.mysql;

import jakarta.persistence.EntityManager;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;

final class MysqlStoreContext {

  private static final String DIALECT = "mysql";
  private static final MetricsCollector NOOP_METRICS_COLLECTOR = new NoopMetricsCollector();

  private final EntityManager em;
  private final MetricsCollector metricsCollector;
  private final int priorityBoostIntervalMinutes;
  private final MysqlConstraintDetector constraintDetector = new MysqlConstraintDetector();

  MysqlStoreContext(EntityManager em, MetricsCollector metricsCollector) {
    this(em, metricsCollector, 15);
  }

  MysqlStoreContext(
      EntityManager em, MetricsCollector metricsCollector, int priorityBoostIntervalMinutes) {
    this.em = em;
    this.metricsCollector = metricsCollector == null ? NOOP_METRICS_COLLECTOR : metricsCollector;
    this.priorityBoostIntervalMinutes = priorityBoostIntervalMinutes;
  }

  EntityManager em() {
    return em;
  }

  MysqlConstraintDetector constraintDetector() {
    return constraintDetector;
  }

  int priorityBoostIntervalMinutes() {
    return priorityBoostIntervalMinutes;
  }

  RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    if (constraintDetector.isDeadlock(e) || constraintDetector.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient MySQL store concurrency failure during " + operation, e);
    }
    return e;
  }

  /**
   * Executes a trusted, package-local SQL count query. Callers must pass hard-coded SQL templates
   * only; runtime values belong in {@code params}.
   */
  // SQL template is a compile-time constant defined in this package; runtime values are bound as
  // JDBC parameters via setParameter.
  @SuppressWarnings("SqlSourceToSinkFlow")
  long countByNative(String sql, Object... params) {
    var query = em.createNativeQuery(sql);
    for (int i = 0; i < params.length; i++) {
      query.setParameter(i + 1, params[i]);
    }
    return ((Number) query.getSingleResult()).longValue();
  }

  /**
   * Executes a trusted, package-local SQL scalar query. Callers must pass hard-coded SQL templates
   * only; runtime values belong in {@code params}.
   */
  // SQL template is a compile-time constant defined in this package; runtime values are bound as
  // JDBC parameters via setParameter.
  @SuppressWarnings("SqlSourceToSinkFlow")
  double doubleByNativeOrZero(String sql, Object... params) {
    var query = em.createNativeQuery(sql);
    for (int i = 0; i < params.length; i++) {
      query.setParameter(i + 1, params[i]);
    }
    Object result = query.getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  <T> T timedStoreOperation(
      String operation, Supplier<T> action, Function<T, String> outcomeFunction) {
    long startNanos = System.nanoTime();
    try {
      T result = action.get();
      recordStoreOperation(operation, outcomeFunction.apply(result), startNanos);
      return result;
    } catch (RatchetTransientStoreException e) {
      recordStoreOperation(operation, "transient_failure", startNanos);
      throw e;
    } catch (RuntimeException e) {
      RuntimeException translated = translateTransientStoreException(operation, e);
      recordStoreOperation(
          operation,
          translated instanceof RatchetTransientStoreException ? "transient_failure" : "failure",
          startNanos);
      throw translated;
    }
  }

  private void recordStoreOperation(String operation, String outcome, long startNanos) {
    metricsCollector.storeOperation(DIALECT, operation, outcome, System.nanoTime() - startNanos);
  }

  private static final class NoopMetricsCollector implements MetricsCollector {
    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(UUID jobId, JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {}

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {}

    @Override
    public void storeOperation(
        String store, String operation, String outcome, long durationNanos) {}
  }
}
