package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutionTuningProvider;

class DefaultExecutionTuningProviderTest {

  @Test
  void supportsSchedulerInternalExecutionTypeNames() {
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(
                execution ->
                    execution
                        .maxConcurrency("DLQ_ALERT", 4)
                        .virtualThreadLimit("WORKFLOW_JOIN", 19))
            .build();

    ExecutionTuningProvider provider = new DefaultExecutionTuningProvider(options);

    assertEquals(4, provider.maxConcurrency("DLQ_ALERT", 2));
    assertEquals(19, provider.virtualThreadLimit("WORKFLOW_JOIN", 1000));
  }

  @Test
  void defaultsThreadingModeToPlatform() {
    ExecutionTuningProvider provider =
        new DefaultExecutionTuningProvider(RatchetOptions.defaults());

    assertEquals(RatchetOptions.ThreadingMode.PLATFORM, provider.defaultThreadingMode());
  }

  @Test
  void protectedConstructorFailsClearlyWhenUsedWithoutInjection() {
    ExecutionTuningProvider provider = new DefaultExecutionTuningProvider();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, provider::defaultThreadingMode);

    assertEquals("RatchetOptions were not injected", thrown.getMessage());
  }
}
