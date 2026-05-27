package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

class ThreadPoolManagerTest {

  private static ThreadPoolManager semaphoreManager(int maxConcurrencyPerType) {
    Map<JobExecutionType, Integer> limits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      limits.put(type, maxConcurrencyPerType);
    }
    return new ThreadPoolManager(
        ExecutorTargets.PLATFORM,
        mock(ExecutorProvider.class),
        mock(MetricsCollector.class),
        ThreadPoolManager.AccountingMode.SEMAPHORE,
        limits);
  }

  private static ThreadPoolManager virtualThreadManager(int limitPerType) {
    Map<JobExecutionType, Integer> limits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      limits.put(type, limitPerType);
    }
    return new ThreadPoolManager(
        ExecutorTargets.VIRTUAL,
        mock(ExecutorProvider.class),
        mock(MetricsCollector.class),
        ThreadPoolManager.AccountingMode.COUNTER,
        limits);
  }

  @Test
  void tryAcquirePermit_whenCapacityAvailable_returnsTrue() {
    ThreadPoolManager manager = semaphoreManager(2);

    assertTrue(manager.tryAcquirePermit(JobExecutionType.SINGLE));
  }

  @Test
  void tryAcquirePermit_atCapacity_returnsFalse() {
    ThreadPoolManager manager = semaphoreManager(1);

    assertTrue(manager.tryAcquirePermit(JobExecutionType.SINGLE), "first acquire");
    assertFalse(manager.tryAcquirePermit(JobExecutionType.SINGLE), "second acquire at capacity");
  }

  @Test
  void releasePermit_restoresCapacity() {
    ThreadPoolManager manager = semaphoreManager(1);

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    manager.releasePermit(JobExecutionType.SINGLE);

    assertTrue(
        manager.tryAcquirePermit(JobExecutionType.SINGLE), "capacity restored after releasePermit");
  }

  @Test
  void releasePermit_withoutAcquire_doesNotIncreaseCapacity() {
    ThreadPoolManager manager = semaphoreManager(1);

    manager.releasePermit(JobExecutionType.SINGLE);

    assertEquals(1, manager.getAvailableCapacity(JobExecutionType.SINGLE));
    assertEquals(0, manager.getActiveThreadCount());
  }

  @Test
  void getAvailableCapacity_reflectsAcquireAndRelease() {
    ThreadPoolManager manager = semaphoreManager(3);

    assertEquals(3, manager.getAvailableCapacity(JobExecutionType.SINGLE));

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    assertEquals(2, manager.getAvailableCapacity(JobExecutionType.SINGLE));

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    assertEquals(1, manager.getAvailableCapacity(JobExecutionType.SINGLE));

    manager.releasePermit(JobExecutionType.SINGLE);
    assertEquals(2, manager.getAvailableCapacity(JobExecutionType.SINGLE));
  }

  @Test
  void canAcceptWork_reflectsAvailability() {
    ThreadPoolManager manager = semaphoreManager(1);

    assertTrue(manager.canAcceptWork(JobExecutionType.SINGLE));

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    assertFalse(manager.canAcceptWork(JobExecutionType.SINGLE));
  }

  @Test
  void getActiveThreadCount_countsAllAcquiredPermits() {
    ThreadPoolManager manager = semaphoreManager(5);

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    manager.tryAcquirePermit(JobExecutionType.BATCH_CHILD);

    assertEquals(3, manager.getActiveThreadCount());
  }

  @Test
  void differentTypesHaveIndependentSemaphores() {
    ThreadPoolManager manager = semaphoreManager(1);

    manager.tryAcquirePermit(JobExecutionType.SINGLE);

    assertTrue(
        manager.tryAcquirePermit(JobExecutionType.BATCH_CHILD),
        "BATCH_CHILD semaphore is independent of SINGLE");
  }

  @Test
  void virtualThreads_tryAcquireRespectsCasLimit() {
    ThreadPoolManager manager = virtualThreadManager(2);

    assertTrue(manager.tryAcquirePermit(JobExecutionType.SINGLE), "first virtual thread");
    assertTrue(manager.tryAcquirePermit(JobExecutionType.SINGLE), "second virtual thread");
    assertFalse(manager.tryAcquirePermit(JobExecutionType.SINGLE), "third exceeds limit of 2");
  }

  @Test
  void virtualThreads_releaseDecrementsCounter() {
    ThreadPoolManager manager = virtualThreadManager(1);

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    manager.releasePermit(JobExecutionType.SINGLE);

    assertTrue(
        manager.tryAcquirePermit(JobExecutionType.SINGLE),
        "counter decremented on release, slot available again");
  }

  @Test
  void virtualThreads_releaseWithoutAcquire_doesNotGoNegative() {
    ThreadPoolManager manager = virtualThreadManager(1);

    manager.releasePermit(JobExecutionType.SINGLE);

    assertEquals(1, manager.getAvailableCapacity(JobExecutionType.SINGLE));
    assertEquals(0, manager.getActiveThreadCount());
  }

  @Test
  void getExecutor_whenNoArgConstructed_throwsClearError() {
    ThreadPoolManager manager = new ThreadPoolManager();

    IllegalStateException error = assertThrows(IllegalStateException.class, manager::getExecutor);

    assertTrue(error.getMessage().contains("ExecutorProvider"));
  }

  @Test
  void getExecutor_delegatesToExecutorProvider() {
    ExecutorProvider provider = mock(ExecutorProvider.class);
    ExecutorService executor = mock(ExecutorService.class);
    when(provider.getJobExecutor(ExecutorTargets.PLATFORM)).thenReturn(Optional.of(executor));
    ThreadPoolManager manager =
        new ThreadPoolManager(
            ExecutorTargets.PLATFORM,
            provider,
            mock(MetricsCollector.class),
            ThreadPoolManager.AccountingMode.SEMAPHORE,
            Map.of(JobExecutionType.SINGLE, 1));

    assertSame(executor, manager.getExecutor());
  }

  @Test
  void utilizationReflectsActivePermitsForSingleTypeAndOverall() {
    ThreadPoolManager manager = semaphoreManager(4);

    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    manager.tryAcquirePermit(JobExecutionType.SINGLE);
    manager.tryAcquirePermit(JobExecutionType.BATCH_CHILD);

    assertEquals(50.0, manager.getUtilization(JobExecutionType.SINGLE));
    assertEquals(3.0 / (JobExecutionType.values().length * 4), manager.getOverallUtilization());
  }

  @Test
  void getThreadPoolHealthReportsSemaphoreState() {
    ThreadPoolManager manager = semaphoreManager(2);
    manager.tryAcquirePermit(JobExecutionType.SINGLE);

    ThreadPoolManager.ThreadPoolHealth health =
        manager.getThreadPoolHealth().get(JobExecutionType.SINGLE);

    assertEquals(JobExecutionType.SINGLE, health.jobType());
    assertFalse(health.isVirtual());
    assertEquals(2, health.corePoolSize());
    assertEquals(2, health.maxPoolSize());
    assertEquals(1, health.activeThreads());
    assertEquals(50.0, health.getUtilizationPercent());
    assertTrue(health.isHealthy());
  }

  @Test
  void virtualThreadHealthReportsVirtualPoolsAndUsage() {
    ThreadPoolManager manager = virtualThreadManager(2);
    assertEquals(ThreadPoolManager.AccountingMode.COUNTER, manager.accountingMode());

    manager.tryAcquirePermit(JobExecutionType.SINGLE);

    assertEquals(1, manager.getActiveThreadCount());
    assertEquals(1.0 / (JobExecutionType.values().length * 2), manager.getOverallUtilization());
    assertEquals(0.0, manager.getUtilization(JobExecutionType.SINGLE));

    ThreadPoolManager.ThreadPoolHealth health =
        manager.getThreadPoolHealth().get(JobExecutionType.SINGLE);
    assertTrue(health.isVirtual());
    assertEquals(0.0, health.getUtilizationPercent());
    assertTrue(health.isHealthy());
  }

  @Test
  void shutdownClearsCapacityAndHealthState() {
    ThreadPoolManager manager = semaphoreManager(2);
    manager.tryAcquirePermit(JobExecutionType.SINGLE);

    manager.shutdown();

    assertEquals(0, manager.getAvailableCapacity(JobExecutionType.SINGLE));
    assertEquals(0, manager.getActiveThreadCount());
    assertEquals(0.0, manager.getOverallUtilization());
    assertTrue(manager.getThreadPoolHealth().isEmpty());
  }

  @Test
  void tryAcquirePermit_afterVirtualShutdown_declinesWork() {
    ThreadPoolManager manager = virtualThreadManager(1);

    manager.shutdown();

    assertFalse(manager.tryAcquirePermit(JobExecutionType.SINGLE));
    assertEquals(0, manager.getAvailableCapacity(JobExecutionType.SINGLE));
  }

  @Test
  void tryAcquirePermit_whenShutdownClearsActiveCounter_declinesWork() throws Exception {
    ThreadPoolManager manager = semaphoreManager(1);
    removeActiveCounter(manager, JobExecutionType.SINGLE);

    assertFalse(manager.tryAcquirePermit(JobExecutionType.SINGLE));
    assertEquals(1, manager.getAvailableCapacity(JobExecutionType.SINGLE));
  }

  @SuppressWarnings("unchecked")
  private static void removeActiveCounter(ThreadPoolManager manager, JobExecutionType jobType)
      throws Exception {
    Field field = ThreadPoolManager.class.getDeclaredField("activeCounts");
    field.setAccessible(true);
    ((Map<JobExecutionType, ?>) field.get(manager)).remove(jobType);
  }
}
