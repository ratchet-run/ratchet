package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

class PoolRegistryTest {

  private static ThreadPoolManager semaphorePool(String name, int limitPerType) {
    Map<JobExecutionType, Integer> limits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      limits.put(type, limitPerType);
    }
    return new ThreadPoolManager(
        name,
        mock(ExecutorProvider.class),
        mock(MetricsCollector.class),
        ThreadPoolManager.AccountingMode.SEMAPHORE,
        limits);
  }

  private static PoolRegistry twoPools(int platformLimit, int virtualLimit) {
    return new PoolRegistry(
        Map.of(
            ExecutorTargets.PLATFORM, semaphorePool(ExecutorTargets.PLATFORM, platformLimit),
            ExecutorTargets.VIRTUAL, semaphorePool(ExecutorTargets.VIRTUAL, virtualLimit)));
  }

  @Test
  void maxAvailableCapacity_takesMaxAcrossPools() {
    PoolRegistry registry = twoPools(2, 5);

    assertEquals(5, registry.maxAvailableCapacity(JobExecutionType.SINGLE));
  }

  @Test
  void canAcceptWork_trueWhenAnyPoolHasCapacity() {
    PoolRegistry registry =
        new PoolRegistry(
            Map.of(ExecutorTargets.PLATFORM, semaphorePool(ExecutorTargets.PLATFORM, 1)));

    assertTrue(registry.canAcceptWork(JobExecutionType.SINGLE));

    registry.pool(ExecutorTargets.PLATFORM).tryAcquirePermit(JobExecutionType.SINGLE);
    assertFalse(registry.canAcceptWork(JobExecutionType.SINGLE));
  }

  @Test
  void pool_unknownName_throws() {
    PoolRegistry registry =
        new PoolRegistry(
            Map.of(ExecutorTargets.PLATFORM, semaphorePool(ExecutorTargets.PLATFORM, 1)));

    assertFalse(registry.hasPool(ExecutorTargets.VIRTUAL));
    assertThrows(IllegalStateException.class, () -> registry.pool(ExecutorTargets.VIRTUAL));
  }

  @Test
  void getThreadPoolHealth_sumsMaxAcrossPools() {
    PoolRegistry registry = twoPools(2, 3);

    ThreadPoolManager.ThreadPoolHealth health =
        registry.getThreadPoolHealth().get(JobExecutionType.SINGLE);

    assertEquals(5, health.maxPoolSize(), "platform 2 + virtual 3");
    assertFalse(health.isVirtual(), "a semaphore pool keeps the aggregate non-virtual");
  }

  @Test
  void getThreadPoolHealth_counterVirtualPoolDoesNotInflatePlatformUtilization() {
    ThreadPoolManager platform = semaphorePool(ExecutorTargets.PLATFORM, 10);
    Map<JobExecutionType, Integer> virtualLimits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      virtualLimits.put(type, 1000);
    }
    ThreadPoolManager virtual =
        new ThreadPoolManager(
            ExecutorTargets.VIRTUAL,
            mock(ExecutorProvider.class),
            mock(MetricsCollector.class),
            ThreadPoolManager.AccountingMode.COUNTER,
            virtualLimits);
    PoolRegistry registry =
        new PoolRegistry(
            Map.of(ExecutorTargets.PLATFORM, platform, ExecutorTargets.VIRTUAL, virtual));

    // Load the virtual (counter) pool heavily and the platform pool lightly.
    for (int i = 0; i < 50; i++) {
      virtual.tryAcquirePermit(JobExecutionType.SINGLE);
    }
    platform.tryAcquirePermit(JobExecutionType.SINGLE);

    ThreadPoolManager.ThreadPoolHealth health =
        registry.getThreadPoolHealth().get(JobExecutionType.SINGLE);

    // Aggregate utilization must reflect only the bounded platform pool (1/10), not the 50 virtual
    // jobs, so the poller does not see a falsely saturated system.
    assertEquals(10, health.maxPoolSize());
    assertEquals(10.0, health.getUtilizationPercent());
  }
}
