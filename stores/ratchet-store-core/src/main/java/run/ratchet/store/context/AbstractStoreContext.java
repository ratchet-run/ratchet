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
package run.ratchet.store.context;

import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.ConstraintDetector;
import run.ratchet.store.util.TransientStoreExceptions;

/**
 * Shared scaffolding for a store module's per-operation context.
 *
 * <p>Holds the metrics collector, the priority-boost window, transient-fault translation, and the
 * timed-operation wrapper that every store dialect needs. Concrete contexts supply only the dialect
 * labels and their {@link ConstraintDetector}; SQL dialects extend {@link AbstractSqlStoreContext}
 * for the native-query helpers.
 *
 * <p>This is store-implementor scaffolding, not application API. The qualified export in {@code
 * module-info} limits it to the bundled store modules.
 */
public abstract class AbstractStoreContext {

  private static final MetricsCollector NOOP_METRICS_COLLECTOR = new NoopMetricsCollector();

  private final MetricsCollector metricsCollector;
  private final int priorityBoostIntervalMinutes;

  protected AbstractStoreContext(
      MetricsCollector metricsCollector, int priorityBoostIntervalMinutes) {
    this.metricsCollector = metricsCollector == null ? NOOP_METRICS_COLLECTOR : metricsCollector;
    this.priorityBoostIntervalMinutes = priorityBoostIntervalMinutes;
  }

  /** Returns the shared no-op metrics collector, for callers that must pass one explicitly. */
  public static MetricsCollector noopMetricsCollector() {
    return NOOP_METRICS_COLLECTOR;
  }

  public int priorityBoostIntervalMinutes() {
    return priorityBoostIntervalMinutes;
  }

  /** Lowercase dialect token used as the metric {@code store} dimension (for example, "mysql"). */
  protected abstract String dialectMetric();

  /** Human-readable dialect label used in exception messages (for example, "MySQL"). */
  protected abstract String dialectLabel();

  public abstract ConstraintDetector constraintDetector();

  public RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    if (e instanceof RatchetTransientStoreException) {
      // A nested operation already translated this. Re-running the detector would walk its cause
      // chain, re-detect the underlying fault, and wrap it a second time — losing the inner
      // operation label. Return the original transient unchanged, matching timedStoreOperation.
      return e;
    }
    RatchetTransientStoreException wrapped =
        TransientStoreExceptions.translateOrNull(
            dialectLabel(), constraintDetector(), operation, e);
    if (wrapped != null) {
      return wrapped;
    }
    return additionalTranslation(operation, e);
  }

  /**
   * Hook for dialect-specific translation applied after the shared transient check. The default
   * returns {@code e} unchanged; stores override to recognize their own non-transient failures.
   */
  protected RuntimeException additionalTranslation(String operation, RuntimeException e) {
    return e;
  }

  public <T> T timedStoreOperation(
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
    metricsCollector.storeOperation(
        dialectMetric(), operation, outcome, System.nanoTime() - startNanos);
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
  }
}
