package run.ratchet.ri.core.internal;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;

public final class ExecutionTargetClaimPlanner {

  private final PoolRegistry poolRegistry;
  private final RatchetOptions options;

  public ExecutionTargetClaimPlanner(PoolRegistry poolRegistry, RatchetOptions options) {
    this.poolRegistry = poolRegistry;
    this.options = options;
  }

  public List<PoolClaimBudget> budgets(JobExecutionType jobType) {
    Map<String, Integer> capacities = poolRegistry.availableCapacitiesByPool(jobType);
    List<PoolClaimBudget> budgets = new ArrayList<>(capacities.size());
    for (Map.Entry<String, Integer> entry : capacities.entrySet()) {
      int availableCapacity = entry.getValue();
      if (availableCapacity <= 0) {
        continue;
      }
      ExecutionTargetFilter filter = filterForPool(entry.getKey(), capacities.keySet());
      if (!filter.matchesNothing()) {
        budgets.add(new PoolClaimBudget(entry.getKey(), availableCapacity, filter));
      }
    }
    return budgets;
  }

  private ExecutionTargetFilter filterForPool(String poolName, Set<String> registeredPools) {
    boolean includeNull = poolName.equals(resolveDefaultPool());
    if (ExecutorTargets.PLATFORM.equals(poolName)) {
      Set<String> nonPlatformPools = new LinkedHashSet<>(registeredPools);
      nonPlatformPools.remove(ExecutorTargets.PLATFORM);
      return ExecutionTargetFilter.excluding(nonPlatformPools, includeNull);
    }

    Set<String> explicitTargets = new LinkedHashSet<>();
    explicitTargets.add(poolName);
    return ExecutionTargetFilter.matching(explicitTargets, includeNull);
  }

  private String resolveDefaultPool() {
    String defaultTarget = options.execution().defaultThreadingMode().target();
    return poolRegistry.hasPool(defaultTarget) ? defaultTarget : ExecutorTargets.PLATFORM;
  }

  public record PoolClaimBudget(
      String poolName, int availableCapacity, ExecutionTargetFilter executionTargetFilter) {}
}
