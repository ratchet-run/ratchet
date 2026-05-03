package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ThreadPoolManagerTest {

  private static ThreadPoolManager semaphoreManager(int maxConcurrencyPerType) {
    Map<JobExecutionType, Integer> limits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      limits.put(type, maxConcurrencyPerType);
    }
    return new ThreadPoolManager(
        mock(ExecutorProvider.class), mock(MetricsCollector.class), false, limits, null);
  }

  private static ThreadPoolManager virtualThreadManager(int limitPerType) {
    Map<JobExecutionType, Integer> limits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      limits.put(type, limitPerType);
    }
    ExecutionTuningProvider tuning = mock(ExecutionTuningProvider.class);
    when(tuning.virtualThreadLimit(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(limitPerType);
    return new ThreadPoolManager(
        mock(ExecutorProvider.class), mock(MetricsCollector.class), true, limits, tuning);
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
}
