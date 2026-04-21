package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutionTuningProvider;
import org.junit.jupiter.api.Test;

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
  void defaultsVirtualThreadsToDisabled() {
    ExecutionTuningProvider provider =
        new DefaultExecutionTuningProvider(RatchetOptions.defaults());

    assertFalse(provider.useVirtualThreads());
  }
}
