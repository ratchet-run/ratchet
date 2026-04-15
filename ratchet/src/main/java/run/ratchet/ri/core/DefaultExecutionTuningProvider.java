package run.ratchet.ri.core;

import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Default execution tuning backed by Ratchet config keys. */
@ApplicationScoped
public class DefaultExecutionTuningProvider implements ExecutionTuningProvider {

  private static final RatchetConfigKey<Boolean> USE_VIRTUAL_THREADS =
      RatchetConfigKey.bool(
          "ratchet.worker.use-virtual-threads",
          "RATCHET_WORKER_USE_VIRTUAL_THREADS",
          "worker.use-virtual-threads",
          "WORKER_USE_VIRTUAL_THREADS",
          false);

  private final RatchetConfig config;

  protected DefaultExecutionTuningProvider() {
    this.config = null;
  }

  @Inject
  public DefaultExecutionTuningProvider(RatchetConfig config) {
    this.config = config;
  }

  @Override
  public boolean useVirtualThreads() {
    return config.get(USE_VIRTUAL_THREADS);
  }

  @Override
  public int maxConcurrency(String executionTypeName, int defaultValue) {
    return config.get(
        RatchetConfigKey.integer(
            "ratchet.thread-pool-size." + executionTypeName.toLowerCase().replace('_', '-'),
            "RATCHET_THREAD_POOL_SIZE_" + executionTypeName,
            "scheduler.thread-pool-size." + executionTypeName.toLowerCase().replace('_', '-'),
            "SCHEDULER_THREAD_POOL_SIZE_" + executionTypeName,
            defaultValue));
  }

  @Override
  public int virtualThreadLimit(String executionTypeName, int defaultValue) {
    return config.get(
        RatchetConfigKey.integer(
            "ratchet.virtual-thread-limit." + executionTypeName.toLowerCase().replace('_', '-'),
            "RATCHET_VIRTUAL_THREAD_LIMIT_" + executionTypeName,
            "virtual-thread-limit." + executionTypeName.toLowerCase().replace('_', '-'),
            "VIRTUAL_THREAD_LIMIT_" + executionTypeName,
            defaultValue));
  }
}
