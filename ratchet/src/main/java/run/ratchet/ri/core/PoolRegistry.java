package run.ratchet.ri.core;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeMap;
import run.ratchet.ri.core.ThreadPoolManager.ThreadPoolHealth;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Name-keyed set of executor pools. Seeded today with exactly two reserved entries (platform and,
 * when configured, virtual); arbitrary named pools can be added later without changing this seam.
 *
 * <p>Per-pool operations ({@link #pool(String)}) drive permit acquire and release once the router
 * has resolved the effective pool for a job. Target-aware capacity views let claim paths size work
 * against each concrete pool, while aggregate health remains available for load reporting.
 */
public class PoolRegistry {

  private final Map<String, ThreadPoolManager> pools;

  public PoolRegistry(Map<String, ThreadPoolManager> pools) {
    this.pools = Map.copyOf(pools);
  }

  /** Returns true when a pool is registered under {@code name}. */
  public boolean hasPool(String name) {
    return pools.containsKey(name);
  }

  /**
   * Returns the pool registered under {@code name}.
   *
   * @throws IllegalStateException if no pool is registered under {@code name}; the router resolves
   *     unknown or unconfigured names to a present pool before reaching here
   */
  public ThreadPoolManager pool(String name) {
    ThreadPoolManager manager = pools.get(name);
    if (manager == null) {
      throw new IllegalStateException("No executor pool registered under name '" + name + "'");
    }
    return manager;
  }

  /**
   * Returns the largest per-type capacity across all pools. This is a legacy aggregate view for
   * callers that only need to know whether any pool can accept work; target-aware claim paths
   * should use {@link #availableCapacitiesByPool(JobExecutionType)}.
   */
  public int maxAvailableCapacity(JobExecutionType jobType) {
    int max = 0;
    for (ThreadPoolManager manager : pools.values()) {
      max = Math.max(max, manager.getAvailableCapacity(jobType));
    }
    return max;
  }

  /** Returns current per-pool capacity for {@code jobType}, ordered by pool name. */
  public Map<String, Integer> availableCapacitiesByPool(JobExecutionType jobType) {
    Map<String, Integer> capacities = new TreeMap<>();
    for (Map.Entry<String, ThreadPoolManager> entry : pools.entrySet()) {
      capacities.put(entry.getKey(), entry.getValue().getAvailableCapacity(jobType));
    }
    return Collections.unmodifiableMap(capacities);
  }

  /** Returns current capacity for {@code jobType} in the named pool. */
  public int availableCapacity(JobExecutionType jobType, String poolName) {
    return pool(poolName).getAvailableCapacity(jobType);
  }

  /** Returns true when any pool has capacity for {@code jobType}. */
  public boolean canAcceptWork(JobExecutionType jobType) {
    return maxAvailableCapacity(jobType) > 0;
  }

  /** Returns true when the named pool has capacity for {@code jobType}. */
  public boolean canAcceptWork(JobExecutionType jobType, String poolName) {
    return pool(poolName).canAcceptWork(jobType);
  }

  /** Per-type health summed across pools. A type is virtual only when every pool reports it so. */
  public Map<JobExecutionType, ThreadPoolHealth> getThreadPoolHealth() {
    Map<JobExecutionType, ThreadPoolHealth> aggregate = new EnumMap<>(JobExecutionType.class);
    for (ThreadPoolManager manager : pools.values()) {
      for (Map.Entry<JobExecutionType, ThreadPoolHealth> entry :
          manager.getThreadPoolHealth().entrySet()) {
        aggregate.merge(entry.getKey(), entry.getValue(), PoolRegistry::sumHealth);
      }
    }
    return aggregate;
  }

  /** Shuts down concurrency tracking for every pool. */
  public void shutdown() {
    for (ThreadPoolManager manager : pools.values()) {
      manager.shutdown();
    }
  }

  private static ThreadPoolHealth sumHealth(ThreadPoolHealth a, ThreadPoolHealth b) {
    return new ThreadPoolHealth(
        a.jobType(),
        a.isVirtual() && b.isVirtual(),
        a.corePoolSize() + b.corePoolSize(),
        a.maxPoolSize() + b.maxPoolSize(),
        boundedActive(a) + boundedActive(b),
        a.queueSize() + b.queueSize(),
        a.rejectionCount() + b.rejectionCount());
  }

  /**
   * Active count that counts toward bounded utilization. A counter-accounted (virtual) pool reports
   * no max capacity, so its active count must not inflate the utilization of the bounded pools it
   * is merged with — otherwise the poller would read a saturated system and throttle while the
   * platform pool still has room.
   */
  private static int boundedActive(ThreadPoolHealth health) {
    return health.isVirtual() ? 0 : health.activeThreads();
  }
}
