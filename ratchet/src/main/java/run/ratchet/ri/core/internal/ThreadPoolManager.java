package run.ratchet.ri.core.internal;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

/**
 * One executor pool's per-{@link JobExecutionType} backpressure, applied before work reaches the
 * executor. A pool is identified by name (see {@link ExecutorTargets}), runs jobs on the executor
 * resolved for that name, and bounds concurrency with one of two accounting modes:
 *
 * <ul>
 *   <li>{@link AccountingMode#SEMAPHORE} — a bounded {@link Semaphore} per type. The default; the
 *       gate blocks once permits are exhausted.
 *   <li>{@link AccountingMode#COUNTER} — a lock-free {@link AtomicInteger} per type compared
 *       against a limit. Cheaper, but only safe when the executor is genuinely virtual-thread
 *       backed; on a small platform-thread executor it admits far more work than the executor can
 *       run.
 * </ul>
 *
 * <p>Several instances live behind {@link PoolRegistry}, one per configured pool.
 */
public class ThreadPoolManager {

  private static final Logger log = Logger.getLogger(ThreadPoolManager.class);

  private static final int DEFAULT_LIMIT = 10;

  /** How a pool bounds concurrency. */
  public enum AccountingMode {
    SEMAPHORE,
    COUNTER
  }

  private final Map<JobExecutionType, Semaphore> semaphores = new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, AtomicInteger> counters =
      new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, AtomicInteger> activeCounts =
      new EnumMap<>(JobExecutionType.class);
  private final Object stateLock = new Object();

  private final String poolName;
  private final ExecutorProvider executorProvider;
  private final MetricsCollector metricsCollector;
  private final AccountingMode accountingMode;
  private final Map<JobExecutionType, Integer> limits;

  protected ThreadPoolManager() {
    this.poolName = ExecutorTargets.PLATFORM;
    this.executorProvider = null;
    this.metricsCollector = null;
    this.accountingMode = AccountingMode.SEMAPHORE;
    this.limits = null;
  }

  public ThreadPoolManager(
      String poolName,
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      AccountingMode accountingMode,
      Map<JobExecutionType, Integer> limits) {
    this.poolName = poolName;
    this.executorProvider = executorProvider;
    this.metricsCollector = metricsCollector;
    this.accountingMode = accountingMode;
    this.limits = limits;

    init();
  }

  public String poolName() {
    return poolName;
  }

  public AccountingMode accountingMode() {
    return accountingMode;
  }

  public boolean canAcceptWork(JobExecutionType jobType) {
    return getAvailableCapacity(jobType) > 0;
  }

  public int getAvailableCapacity(JobExecutionType jobType) {
    if (accountingMode == AccountingMode.COUNTER) {
      // AtomicInteger.get() is intrinsically thread-safe; no lock needed for the counter path.
      AtomicInteger counter = counters.get(jobType);
      if (counter == null) {
        return 0;
      }
      return Math.max(0, limit(jobType) - counter.get());
    }

    // Semaphore.availablePermits() is not atomic with check-and-modify; retain lock for this path.
    synchronized (stateLock) {
      Semaphore semaphore = semaphores.get(jobType);
      if (semaphore == null) {
        return 0;
      }
      return semaphore.availablePermits();
    }
  }

  public int getActiveThreadCount() {
    if (accountingMode == AccountingMode.COUNTER) {
      int totalActive = 0;
      for (AtomicInteger counter : counters.values()) {
        totalActive += counter.get();
      }
      return totalActive;
    }

    synchronized (stateLock) {
      int totalActive = 0;
      for (AtomicInteger activeCount : activeCounts.values()) {
        totalActive += activeCount.get();
      }
      return totalActive;
    }
  }

  /** Returns the executor that backs this pool, resolved (lazily) by pool name. */
  public ExecutorService getExecutor() {
    return requireExecutorProvider()
        .getJobExecutor(poolName)
        .orElseThrow(
            () ->
                new IllegalStateException("No executor is configured for pool '" + poolName + "'"));
  }

  public double getOverallUtilization() {
    synchronized (stateLock) {
      if (accountingMode == AccountingMode.COUNTER) {
        int totalActive = 0;
        int totalMax = 0;
        for (JobExecutionType jobType : JobExecutionType.values()) {
          AtomicInteger counter = counters.get(jobType);
          if (counter != null) {
            totalActive += counter.get();
            totalMax += limit(jobType);
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
          totalMax += limit(jobType);
        }
      }
      return totalMax > 0 ? (double) totalActive / totalMax : 0.0;
    }
  }

  public double getUtilization(JobExecutionType jobType) {
    synchronized (stateLock) {
      if (accountingMode == AccountingMode.COUNTER) {
        return 0;
      }

      AtomicInteger activeCount = activeCounts.get(jobType);
      if (activeCount != null) {
        int active = activeCount.get();
        int max = limit(jobType);
        if (max == 0) {
          return 0;
        }
        return (double) active / max * 100;
      }
      return 0;
    }
  }

  public void releasePermit(JobExecutionType jobType) {
    synchronized (stateLock) {
      if (accountingMode == AccountingMode.COUNTER) {
        AtomicInteger counter = counters.get(jobType);
        if (counter != null) {
          decrementIfPositive(counter);
        }
        return;
      }

      Semaphore semaphore = semaphores.get(jobType);
      AtomicInteger activeCount = activeCounts.get(jobType);
      if (semaphore != null && activeCount != null && decrementIfPositive(activeCount)) {
        semaphore.release();
      }
    }
  }

  public boolean tryAcquirePermit(JobExecutionType jobType) {
    synchronized (stateLock) {
      if (accountingMode == AccountingMode.COUNTER) {
        AtomicInteger counter = counters.get(jobType);
        if (counter == null) {
          return false;
        }
        int maxLimit = limit(jobType);
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

      Semaphore semaphore = semaphores.get(jobType);
      AtomicInteger activeCount = activeCounts.get(jobType);
      if (semaphore != null && activeCount != null && semaphore.tryAcquire()) {
        activeCount.incrementAndGet();
        return true;
      }
      return false;
    }
  }

  public Map<JobExecutionType, ThreadPoolHealth> getThreadPoolHealth() {
    synchronized (stateLock) {
      Map<JobExecutionType, ThreadPoolHealth> health = new EnumMap<>(JobExecutionType.class);

      for (JobExecutionType jobType : JobExecutionType.values()) {
        if (accountingMode == AccountingMode.COUNTER) {
          AtomicInteger counter = counters.get(jobType);
          int active = counter != null ? counter.get() : 0;
          health.put(jobType, new ThreadPoolHealth(jobType, true, 0, 0, active, 0, 0));
        } else {
          Semaphore semaphore = semaphores.get(jobType);
          AtomicInteger activeCount = activeCounts.get(jobType);

          if (semaphore != null) {
            int maxConcurrency = limit(jobType);
            int active = activeCount != null ? activeCount.get() : 0;

            health.put(
                jobType,
                new ThreadPoolHealth(jobType, false, maxConcurrency, maxConcurrency, active, 0, 0));
          }
        }
      }
      return health;
    }
  }

  /**
   * Clears concurrency tracking state. The underlying {@link ExecutorService} instances are owned
   * by {@link ExecutorProvider} and shut down through its lifecycle, not here.
   */
  public void shutdown() {
    synchronized (stateLock) {
      semaphores.clear();
      counters.clear();
      activeCounts.clear();
    }
  }

  private void init() {
    for (JobExecutionType jobType : JobExecutionType.values()) {
      if (accountingMode == AccountingMode.COUNTER) {
        counters.put(jobType, new AtomicInteger(0));
      } else {
        semaphores.put(jobType, new Semaphore(limit(jobType)));
        activeCounts.put(jobType, new AtomicInteger(0));
      }
    }

    log.infof(
        "Thread pool '%s' initialized with %s accounting",
        poolName, accountingMode == AccountingMode.COUNTER ? "counter-based" : "semaphore-based");
  }

  private ExecutorProvider requireExecutorProvider() {
    if (executorProvider == null) {
      throw new IllegalStateException(
          "ThreadPoolManager was constructed without an ExecutorProvider; use the CDI producer "
              + "or public constructor before requesting an executor.");
    }
    return executorProvider;
  }

  private int limit(JobExecutionType jobType) {
    if (limits == null) {
      return DEFAULT_LIMIT;
    }
    return limits.getOrDefault(jobType, DEFAULT_LIMIT);
  }

  private static boolean decrementIfPositive(AtomicInteger counter) {
    while (true) {
      int current = counter.get();
      if (current <= 0) {
        return false;
      }
      if (counter.compareAndSet(current, current - 1)) {
        return true;
      }
    }
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
