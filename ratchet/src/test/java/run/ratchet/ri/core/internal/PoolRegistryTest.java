/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

class PoolRegistryTest {

  private static final Map<JobExecutionType, Integer> PLATFORM_LIMITS =
      Map.of(
          JobExecutionType.SINGLE, 20,
          JobExecutionType.RECURRING, 5,
          JobExecutionType.BATCH_CHILD, 30,
          JobExecutionType.BATCH_PARENT, 2,
          JobExecutionType.CHAIN_STEP, 10,
          JobExecutionType.WORKFLOW_BRANCH, 10,
          JobExecutionType.WORKFLOW_JOIN, 10);

  @Test
  void createWithoutVirtualPoolUsesPlatformFallbackLimits() {
    PoolRegistry registry = create(optionsWithoutOverrides(false), fallbackTuning(), false);

    for (JobExecutionType type : JobExecutionType.values()) {
      assertEquals(
          Map.of(ExecutorTargets.PLATFORM, PLATFORM_LIMITS.get(type)),
          registry.availableCapacitiesByPool(type));
      ThreadPoolManager.ThreadPoolHealth health =
          registry.pool(ExecutorTargets.PLATFORM).getThreadPoolHealth().get(type);
      assertEquals(PLATFORM_LIMITS.get(type).intValue(), health.maxPoolSize());
      assertFalse(health.isVirtual());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createVirtualPoolUsesFallbackLimitsAndConfiguredAccounting(boolean counterAccounting) {
    PoolRegistry registry =
        create(optionsWithoutOverrides(counterAccounting), fallbackTuning(), true);

    for (JobExecutionType type : JobExecutionType.values()) {
      assertEquals(
          Map.of(
              ExecutorTargets.PLATFORM, PLATFORM_LIMITS.get(type), ExecutorTargets.VIRTUAL, 1000),
          registry.availableCapacitiesByPool(type));
      ThreadPoolManager virtual = registry.pool(ExecutorTargets.VIRTUAL);
      assertTrue(virtual.tryAcquirePermit(type));
      ThreadPoolManager.ThreadPoolHealth health = virtual.getThreadPoolHealth().get(type);
      assertEquals(counterAccounting, health.isVirtual());
      assertEquals(counterAccounting ? 0 : 1000, health.maxPoolSize());
      assertEquals(1, health.activeThreads());
      assertEquals(999, virtual.getAvailableCapacity(type));
      assertFalse(
          registry.pool(ExecutorTargets.PLATFORM).getThreadPoolHealth().get(type).isVirtual());
    }
  }

  @Test
  void createHonorsOptionsAndTuningOverrides() {
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(execution -> execution.maxConcurrency("SINGLE", 7))
            .build();
    ExecutionTuningProvider tuning = fallbackTuning();
    PoolRegistry fromOptions = create(options, tuning, false);
    assertEquals(
        7, fromOptions.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM));

    when(tuning.maxConcurrency("SINGLE", 7)).thenReturn(11);
    when(tuning.virtualThreadLimit("SINGLE", 1000)).thenReturn(17);
    PoolRegistry fromTuning = create(options, tuning, true);
    for (JobExecutionType type : JobExecutionType.values()) {
      assertEquals(
          Map.of(
              ExecutorTargets.PLATFORM,
              type == JobExecutionType.SINGLE ? 11 : PLATFORM_LIMITS.get(type),
              ExecutorTargets.VIRTUAL,
              type == JobExecutionType.SINGLE ? 17 : 1000),
          fromTuning.availableCapacitiesByPool(type));
    }
  }

  private static RatchetOptions optionsWithoutOverrides(boolean counterAccounting) {
    RatchetOptions options = mock(RatchetOptions.class);
    when(options.execution())
        .thenReturn(
            new RatchetOptions.ExecutionOptions(
                RatchetOptions.ThreadingMode.PLATFORM,
                100,
                Map.of(),
                Map.of(),
                Map.of(),
                "platform",
                "scheduled",
                "virtual",
                counterAccounting));
    return options;
  }

  private static ExecutionTuningProvider fallbackTuning() {
    ExecutionTuningProvider tuning = mock(ExecutionTuningProvider.class);
    when(tuning.maxConcurrency(anyString(), anyInt())).thenAnswer(call -> call.getArgument(1));
    when(tuning.virtualThreadLimit(anyString(), anyInt())).thenAnswer(call -> call.getArgument(1));
    return tuning;
  }

  private static PoolRegistry create(
      RatchetOptions options, ExecutionTuningProvider tuning, boolean virtualPool) {
    return PoolRegistry.create(
        options, mock(ExecutorProvider.class), mock(MetricsCollector.class), tuning, virtualPool);
  }

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
  void availableCapacitiesByPool_reportsEachPoolSeparately() {
    PoolRegistry registry = twoPools(2, 5);

    Map<String, Integer> capacities = registry.availableCapacitiesByPool(JobExecutionType.SINGLE);

    assertEquals(2, capacities.get(ExecutorTargets.PLATFORM));
    assertEquals(5, capacities.get(ExecutorTargets.VIRTUAL));
  }

  @Test
  void canAcceptWork_trueWhenAnyPoolHasCapacity() {
    PoolRegistry registry =
        new PoolRegistry(
            Map.of(ExecutorTargets.PLATFORM, semaphorePool(ExecutorTargets.PLATFORM, 1)));

    assertTrue(registry.canAcceptWork(JobExecutionType.SINGLE));

    registry.pool(ExecutorTargets.PLATFORM).tryAcquirePermit(JobExecutionType.SINGLE);
    assertFalse(registry.canAcceptWork(JobExecutionType.SINGLE));
    assertFalse(registry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM));
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
