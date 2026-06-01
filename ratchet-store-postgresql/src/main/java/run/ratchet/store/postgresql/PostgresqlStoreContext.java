/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.postgresql;

import jakarta.persistence.EntityManager;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.util.StatusClassifier;

final class PostgresqlStoreContext {

  private static final String DIALECT = "postgresql";
  private static final MetricsCollector NOOP_METRICS_COLLECTOR = new NoopMetricsCollector();

  private final EntityManager em;
  private final MetricsCollector metricsCollector;
  private final int priorityBoostIntervalMinutes;
  private final PostgresqlConstraintDetector constraintDetector =
      new PostgresqlConstraintDetector();

  PostgresqlStoreContext(EntityManager em) {
    this(em, NOOP_METRICS_COLLECTOR, 15);
  }

  PostgresqlStoreContext(EntityManager em, int priorityBoostIntervalMinutes) {
    this(em, NOOP_METRICS_COLLECTOR, priorityBoostIntervalMinutes);
  }

  PostgresqlStoreContext(
      EntityManager em, MetricsCollector metricsCollector, int priorityBoostIntervalMinutes) {
    this.em = em;
    this.metricsCollector = metricsCollector;
    this.priorityBoostIntervalMinutes = priorityBoostIntervalMinutes;
  }

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return StatusClassifier.isPollerExecutable(jobType);
  }

  static boolean isLiveStatus(JobStatus status) {
    return StatusClassifier.isLiveStatus(status);
  }

  static boolean isTerminalStatus(JobStatus status) {
    return StatusClassifier.isTerminalStatus(status);
  }

  static JobStatus effectiveStatus(JobStatus status) {
    return StatusClassifier.effectiveStatus(status);
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

  /**
   * Runs a compile-time SQL count query with positional parameters.
   *
   * <p>The {@code sql} argument must be a package-owned constant. Never pass user-controlled SQL
   * fragments; bind user values through {@code params}.
   */
  long countByNative(String sql, Object... params) {
    try {
      var query = em.createNativeQuery(sql);
      for (int i = 0; i < params.length; i++) {
        query.setParameter(i + 1, params[i]);
      }
      return ((Number) query.getSingleResult()).longValue();
    } catch (RuntimeException e) {
      throw translateTransientStoreException("count by native SQL", e);
    }
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
  }
}
