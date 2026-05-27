package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.MetricsCollector;

/**
 * Resolves a job's requested execution target to an effective pool name, exactly once, before a
 * permit is acquired.
 *
 * <p>Resolution: a null target inherits the configured default threading mode. A target with no
 * matching pool — most commonly {@code virtual} when no virtual executor is configured — falls back
 * to {@link ExecutorTargets#PLATFORM}, recorded with a one-time warning per distinct target and a
 * metric. The fallback lives here rather than in {@link run.ratchet.spi.ExecutorProvider} so the
 * SPI stays an honest keyed lookup.
 */
@ApplicationScoped
public class ExecutionTargetRouter {

  private static final Logger log = Logger.getLogger(ExecutionTargetRouter.class);

  private final PoolRegistry poolRegistry;
  private final ExecutionTuningProvider executionTuningProvider;
  private final MetricsCollector metricsCollector;
  private final Set<String> warnedTargets = ConcurrentHashMap.newKeySet();

  protected ExecutionTargetRouter() {
    this.poolRegistry = null;
    this.executionTuningProvider = null;
    this.metricsCollector = null;
  }

  @Inject
  public ExecutionTargetRouter(
      PoolRegistry poolRegistry,
      ExecutionTuningProvider executionTuningProvider,
      MetricsCollector metricsCollector) {
    this.poolRegistry = poolRegistry;
    this.executionTuningProvider = executionTuningProvider;
    this.metricsCollector = metricsCollector;
  }

  /**
   * Resolves the pool a job runs on.
   *
   * @param requestedTarget the job's target, or null to inherit the default threading mode
   * @return the name of a pool guaranteed to be present in the registry
   */
  public String resolve(String requestedTarget) {
    String requested =
        requestedTarget != null
            ? requestedTarget
            : executionTuningProvider.defaultThreadingMode().target();

    if (poolRegistry.hasPool(requested)) {
      return requested;
    }

    if (warnedTargets.add(requested)) {
      log.warnf(
          "Execution target '%s' has no configured pool; jobs requesting it run on '%s'."
              + " Configure 'ratchet.worker.virtual-executor-jndi' to add a virtual pool.",
          requested, ExecutorTargets.PLATFORM);
    }
    if (metricsCollector != null) {
      metricsCollector.executionTargetFallback(requested, ExecutorTargets.PLATFORM);
    }
    return ExecutorTargets.PLATFORM;
  }
}
