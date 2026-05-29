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
