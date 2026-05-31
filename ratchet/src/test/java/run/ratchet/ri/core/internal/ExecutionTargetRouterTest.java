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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.RatchetOptions.ThreadingMode;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.MetricsCollector;

@ExtendWith(MockitoExtension.class)
class ExecutionTargetRouterTest {

  @Mock private PoolRegistry poolRegistry;
  @Mock private ExecutionTuningProvider executionTuningProvider;
  @Mock private MetricsCollector metricsCollector;

  private ExecutionTargetRouter router;

  @BeforeEach
  void setUp() {
    router = new ExecutionTargetRouter(poolRegistry, executionTuningProvider, metricsCollector);
  }

  @Test
  void nullTarget_resolvesToDefaultThreadingMode() {
    when(executionTuningProvider.defaultThreadingMode()).thenReturn(ThreadingMode.PLATFORM);
    when(poolRegistry.hasPool(ExecutorTargets.PLATFORM)).thenReturn(true);

    assertEquals(ExecutorTargets.PLATFORM, router.resolve(null));
    verifyNoInteractions(metricsCollector);
  }

  @Test
  void configuredTarget_resolvesToItself() {
    when(poolRegistry.hasPool(ExecutorTargets.VIRTUAL)).thenReturn(true);

    assertEquals(ExecutorTargets.VIRTUAL, router.resolve(ExecutorTargets.VIRTUAL));
    verifyNoInteractions(metricsCollector);
  }

  @Test
  void virtualWithNoPool_fallsBackToPlatformAndRecordsMetric() {
    when(poolRegistry.hasPool(ExecutorTargets.VIRTUAL)).thenReturn(false);

    assertEquals(ExecutorTargets.PLATFORM, router.resolve(ExecutorTargets.VIRTUAL));
    verify(metricsCollector)
        .executionTargetFallback(ExecutorTargets.VIRTUAL, ExecutorTargets.PLATFORM);
  }

  @Test
  void defaultVirtualWithNoPool_recordsMetricOnEveryCall() {
    when(executionTuningProvider.defaultThreadingMode()).thenReturn(ThreadingMode.VIRTUAL);
    when(poolRegistry.hasPool(ExecutorTargets.VIRTUAL)).thenReturn(false);

    assertEquals(ExecutorTargets.PLATFORM, router.resolve(null));
    assertEquals(ExecutorTargets.PLATFORM, router.resolve(null));

    // The metric counts every fallback (a misconfiguration signal); the warning is once-per-target.
    verify(metricsCollector, times(2))
        .executionTargetFallback(ExecutorTargets.VIRTUAL, ExecutorTargets.PLATFORM);
  }
}
