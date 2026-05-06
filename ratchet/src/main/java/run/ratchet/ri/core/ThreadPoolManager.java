package run.ratchet.ri.core;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

/** Manages per-{@link JobExecutionType} concurrency limits before work reaches the executor. */
public class ThreadPoolManager {

  private static final Logger log = Logger.getLogger(ThreadPoolManager.class);

  private static final int DEFAULT_VIRTUAL_THREAD_LIMIT = 1000;

  private final Map<JobExecutionType, Semaphore> concurrencyLimits =
      new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, AtomicInteger> virtualThreadCounts =
      new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, Integer> virtualThreadLimits =
      new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, AtomicInteger> activeCounts =
      new EnumMap<>(JobExecutionType.class);

  private final ExecutorProvider executorProvider;
  private final MetricsCollector metricsCollector;
  private final boolean useVirtualThreads;
  private final Map<JobExecutionType, Integer> maxConcurrencyMap;
  private final ExecutionTuningProvider executionTuningProvider;

  protected ThreadPoolManager() {
    this.executorProvider = null;
    this.metricsCollector = null;
    this.useVirtualThreads = false;
    this.maxConcurrencyMap = null;
    this.executionTuningProvider = null;
  }

  public ThreadPoolManager(
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      boolean useVirtualThreads,
      Map<JobExecutionType, Integer> maxConcurrencyMap,
      ExecutionTuningProvider executionTuningProvider) {
    this.executorProvider = executorProvider;
    this.metricsCollector = metricsCollector;
    this.useVirtualThreads = useVirtualThreads;
    this.maxConcurrencyMap = maxConcurrencyMap;
    this.executionTuningProvider = executionTuningProvider;

    init();
  }

  public boolean canAcceptWork(JobExecutionType jobType) {
    return getAvailableCapacity(jobType) > 0;
  }

  public int getAvailableCapacity(JobExecutionType jobType) {
    if (useVirtualThreads) {
      AtomicInteger counter = virtualThreadCounts.get(jobType);
      if (counter == null) {
        return DEFAULT_VIRTUAL_THREAD_LIMIT;
      }
      int maxLimit = virtualThreadLimits.getOrDefault(jobType, DEFAULT_VIRTUAL_THREAD_LIMIT);
      return Math.max(0, maxLimit - counter.get());
    }

    Semaphore semaphore = concurrencyLimits.get(jobType);
    if (semaphore == null) {
      return 0;
    }
    return semaphore.availablePermits();
  }

  public int getActiveThreadCount() {
    if (useVirtualThreads) {
      int totalActive = 0;
      for (AtomicInteger counter : virtualThreadCounts.values()) {
        totalActive += counter.get();
      }
      return totalActive;
    }

    int totalActive = 0;
    for (AtomicInteger activeCount : activeCounts.values()) {
      totalActive += activeCount.get();
    }
    return totalActive;
  }

  public ExecutorService getExecutor(JobExecutionType jobType) {
    return executorProvider.getJobExecutor();
  }

  public double getOverallUtilization() {
    if (useVirtualThreads) {
      int totalActive = 0;
      int totalMax = 0;
      for (JobExecutionType jobType : JobExecutionType.values()) {
        AtomicInteger counter = virtualThreadCounts.get(jobType);
        if (counter != null) {
          totalActive += counter.get();
          totalMax += virtualThreadLimits.getOrDefault(jobType, DEFAULT_VIRTUAL_THREAD_LIMIT);
        }
      }
      return totalMax > 0 ? (double) totalActive / totalMax : 0.0;
    }

    int totalActive = 0;
    int totalMax = 0;
    for (JobExecutionType jobType : JobExecutionType.values()) {
      AtomicInteger activeCount = activeCounts.get(jobType);
      if (activeCount != null) {
        totalActive += activeCount.get();
        totalMax += getMaxConcurrency(jobType);
      }
    }
    return totalMax > 0 ? (double) totalActive / totalMax : 0.0;
  }

  public double getUtilization(JobExecutionType jobType) {
    if (useVirtualThreads) {
      return 0;
    }

    AtomicInteger activeCount = activeCounts.get(jobType);
    if (activeCount != null) {
      int active = activeCount.get();
      int max = getMaxConcurrency(jobType);
      if (max == 0) {
        return 0;
      }
      return (double) active / max * 100;
    }
    return 0;
  }

  public void releasePermit(JobExecutionType jobType) {
    if (useVirtualThreads) {
      AtomicInteger counter = virtualThreadCounts.get(jobType);
      if (counter != null) {
        counter.decrementAndGet();
      }
      return;
    }

    Semaphore semaphore = concurrencyLimits.get(jobType);
    if (semaphore != null) {
      semaphore.release();
      activeCounts.get(jobType).decrementAndGet();
    }
  }

  public boolean tryAcquirePermit(JobExecutionType jobType) {
    if (useVirtualThreads) {
      AtomicInteger counter = virtualThreadCounts.get(jobType);
      if (counter == null) {
        return true;
      }
      int maxLimit = virtualThreadLimits.getOrDefault(jobType, DEFAULT_VIRTUAL_THREAD_LIMIT);
      while (true) {
        int current = counter.get();
        if (current >= maxLimit) {
          return false;
        }
        if (counter.compareAndSet(current, current + 1)) {
          return true;
        }
      }
    }

    Semaphore semaphore = concurrencyLimits.get(jobType);
    if (semaphore != null && semaphore.tryAcquire()) {
      activeCounts.get(jobType).incrementAndGet();
      return true;
    }
    return false;
  }

  public boolean isUseVirtualThreads() {
    return useVirtualThreads;
  }

  public Map<JobExecutionType, ThreadPoolHealth> getThreadPoolHealth() {
    Map<JobExecutionType, ThreadPoolHealth> health = new EnumMap<>(JobExecutionType.class);

    for (JobExecutionType jobType : JobExecutionType.values()) {
      if (useVirtualThreads) {
        health.put(jobType, new ThreadPoolHealth(jobType, true, 0, 0, 0, 0, 0));
      } else {
        Semaphore semaphore = concurrencyLimits.get(jobType);
        AtomicInteger activeCount = activeCounts.get(jobType);

        if (semaphore != null) {
          int maxConcurrency = getMaxConcurrency(jobType);
          int active = activeCount != null ? activeCount.get() : 0;

          health.put(
              jobType,
              new ThreadPoolHealth(jobType, false, maxConcurrency, maxConcurrency, active, 0, 0));
        }
      }
    }
    return health;
  }

  /**
   * Clears concurrency tracking state. The underlying {@link ExecutorService} instances are owned
   * by {@link ExecutorProvider} and shut down through its lifecycle, not here.
   */
  public void shutdown() {
    concurrencyLimits.clear();
    virtualThreadCounts.clear();
    virtualThreadLimits.clear();
    activeCounts.clear();
  }

  private void init() {
    for (JobExecutionType jobType : JobExecutionType.values()) {
      int maxConcurrency = getMaxConcurrency(jobType);

      if (useVirtualThreads) {
        virtualThreadCounts.put(jobType, new AtomicInteger(0));
        virtualThreadLimits.put(jobType, getVirtualThreadLimit(jobType));
      } else {
        concurrencyLimits.put(jobType, new Semaphore(maxConcurrency));
        activeCounts.put(jobType, new AtomicInteger(0));
      }
    }

    log.infof(
        "Thread pool manager initialized with %s",
        (useVirtualThreads
            ? "executor-backed virtual-thread-style backpressure limits"
            : "managed executors with semaphore-based limiting"));
  }

  private int getVirtualThreadLimit(JobExecutionType jobType) {
    if (executionTuningProvider != null) {
      return executionTuningProvider.virtualThreadLimit(
          jobType.name(), DEFAULT_VIRTUAL_THREAD_LIMIT);
    }
    return DEFAULT_VIRTUAL_THREAD_LIMIT;
  }

  private int getMaxConcurrency(JobExecutionType jobType) {
    return maxConcurrencyMap.getOrDefault(jobType, 10);
  }

  /** Health snapshot for a single thread pool. */
  public record ThreadPoolHealth(
      JobExecutionType jobType,
      boolean isVirtual,
      int corePoolSize,
      int maxPoolSize,
      int activeThreads,
      int queueSize,
      long rejectionCount) {

    public double getUtilizationPercent() {
      if (isVirtual || maxPoolSize == 0) {
        return 0;
      }
      return (double) activeThreads / maxPoolSize * 100;
    }

    public boolean isHealthy() {
      if (isVirtual) {
        return true;
      }
      return getUtilizationPercent() < 90 && rejectionCount < 50;
    }
  }
}
