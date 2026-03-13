package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Centralized manager for job-type-specific thread pools, providing resource isolation and
 * preventing job type starvation in the scheduler system.
 *
 * <p>The ThreadPoolManager implements a sophisticated resource management strategy:
 *
 * <ul>
 *   <li><b>Type Isolation:</b> Each execution type gets its own concurrency limit to prevent
 *       starvation
 *   <li><b>Capacity Management:</b> Semaphore-based permits ensure pools don't become overloaded
 *   <li><b>Virtual Thread Support:</b> Seamlessly switches between platform and virtual threads
 * </ul>
 *
 * @see JobExecutionType for the internal execution categories
 * @see JobExecutionCoordinator for job submission to these pools
 */
public class ThreadPoolManager {

  private static final Logger log = Logger.getLogger(ThreadPoolManager.class.getName());

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

  // Required by CDI proxy
  protected ThreadPoolManager() {
    this.executorProvider = null;
    this.metricsCollector = null;
    this.useVirtualThreads = false;
    this.maxConcurrencyMap = null;
  }

  /**
   * Creates a new ThreadPoolManager.
   *
   * @param executorProvider provides executor services for job execution
   * @param metricsCollector collects metrics about pool utilization
   * @param useVirtualThreads whether to use virtual threads instead of platform threads
   * @param maxConcurrencyMap configured max concurrency per job type
   */
  public ThreadPoolManager(
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      boolean useVirtualThreads,
      Map<JobExecutionType, Integer> maxConcurrencyMap) {
    this.executorProvider = executorProvider;
    this.metricsCollector = metricsCollector;
    this.useVirtualThreads = useVirtualThreads;
    this.maxConcurrencyMap = maxConcurrencyMap;

    init();
  }

  /**
   * Checks if the executor for the given job type can accept more work.
   *
   * @param jobType the type of job to check capacity for
   * @return true if the pool can safely accept more work
   */
  public boolean canAcceptWork(JobExecutionType jobType) {
    return getAvailableCapacity(jobType) > 0;
  }

  /**
   * Returns the currently available execution capacity for the given job type.
   *
   * @param jobType the type of job to inspect
   * @return the number of additional jobs that can be accepted immediately
   */
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

  /**
   * Gets the total number of active threads across all pools.
   *
   * @return the count of active threads
   */
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

  /**
   * Gets the appropriate executor for the given job type.
   *
   * @param jobType the type of job needing an executor
   * @return the ExecutorService for this job type
   * @throws IllegalStateException if called when virtual threads are enabled
   */
  public ExecutorService getExecutor(JobExecutionType jobType) {
    if (useVirtualThreads) {
      throw new IllegalStateException(
          "getExecutor() should not be called when virtual threads are enabled. "
              + "Create virtual threads directly using Thread.ofVirtual() instead.");
    }
    log.info("Providing managed executor for job type: " + jobType);
    return executorProvider.getJobExecutor();
  }

  /**
   * Gets the overall utilization ratio across all thread pools.
   *
   * @return the overall utilization ratio (0.0 to 1.0)
   */
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

  /**
   * Gets current utilization percentage for the given job type's thread pool.
   *
   * @param jobType the type of job to get utilization for
   * @return the utilization percentage (0-100)
   */
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

  /**
   * Releases a permit after work completion for the given job type.
   *
   * @param jobType the type of job
   */
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

  /**
   * Acquires a permit to execute work for the given job type.
   *
   * @param jobType the type of job
   * @return true if permit was acquired, false if no permits available
   */
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

  /**
   * Returns whether virtual threads are enabled.
   *
   * @return true if using virtual threads
   */
  public boolean isUseVirtualThreads() {
    return useVirtualThreads;
  }

  /**
   * Gets detailed health information for all thread pools.
   *
   * @return a map of job types to their thread pool health information
   */
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

  /** Performs cleanup during application shutdown. */
  public void shutdown() {
    log.info("Shutting down managed thread pools...");
    log.info("Thread pool manager shutdown complete");
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

    log.info(
        "Thread pool manager initialized with "
            + (useVirtualThreads
                ? "virtual threads (with backpressure limits)"
                : "managed executors with semaphore-based limiting"));
  }

  private int getVirtualThreadLimit(JobExecutionType jobType) {
    String envKey = "VIRTUAL_THREAD_LIMIT_" + jobType.name();
    String value = System.getenv(envKey);
    if (value != null && !value.isBlank()) {
      try {
        return Integer.parseInt(value.trim());
      } catch (NumberFormatException e) {
        log.warning(
            "Invalid "
                + envKey
                + " value: "
                + value
                + ", using default: "
                + DEFAULT_VIRTUAL_THREAD_LIMIT);
      }
    }
    return DEFAULT_VIRTUAL_THREAD_LIMIT;
  }

  private int getMaxConcurrency(JobExecutionType jobType) {
    return maxConcurrencyMap.getOrDefault(jobType, 10);
  }

  /**
   * Health information for a thread pool.
   *
   * @param jobType the job type this health record applies to
   * @param isVirtual true if using virtual threads
   * @param corePoolSize the base number of threads in the pool
   * @param maxPoolSize the maximum threads allowed
   * @param activeThreads the number of threads currently executing jobs
   * @param queueSize the number of jobs waiting in queue
   * @param rejectionCount total number of jobs rejected due to capacity
   */
  public record ThreadPoolHealth(
      JobExecutionType jobType,
      boolean isVirtual,
      int corePoolSize,
      int maxPoolSize,
      int activeThreads,
      int queueSize,
      long rejectionCount) {

    /**
     * Calculates the utilization percentage for this thread pool.
     *
     * @return the utilization percentage (0-100), or 0 for virtual threads
     */
    public double getUtilizationPercent() {
      if (isVirtual || maxPoolSize == 0) {
        return 0;
      }
      return (double) activeThreads / maxPoolSize * 100;
    }

    /**
     * Determines if this thread pool is in a healthy state.
     *
     * @return true if the pool is healthy
     */
    public boolean isHealthy() {
      if (isVirtual) {
        return true;
      }
      return getUtilizationPercent() < 90 && rejectionCount < 50;
    }
  }
}
