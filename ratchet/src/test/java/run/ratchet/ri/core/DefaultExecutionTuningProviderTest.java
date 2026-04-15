package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import run.ratchet.ri.config.DefaultRatchetConfig;
import run.ratchet.ri.config.EnvironmentRatchetConfigSource;
import run.ratchet.spi.ExecutionTuningProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DefaultExecutionTuningProviderTest {

  @AfterEach
  void clearProperties() {
    System.clearProperty("RATCHET_THREAD_POOL_SIZE_DLQ_ALERT");
    System.clearProperty("RATCHET_VIRTUAL_THREAD_LIMIT_WORKFLOW_JOIN");
    System.clearProperty("RATCHET_WORKER_USE_VIRTUAL_THREADS");
  }

  @Test
  void supportsSchedulerInternalExecutionTypeNames() {
    System.setProperty("RATCHET_THREAD_POOL_SIZE_DLQ_ALERT", "4");
    System.setProperty("RATCHET_VIRTUAL_THREAD_LIMIT_WORKFLOW_JOIN", "19");

    ExecutionTuningProvider provider =
        new DefaultExecutionTuningProvider(
            new DefaultRatchetConfig(new EnvironmentRatchetConfigSource()));

    assertEquals(4, provider.maxConcurrency("DLQ_ALERT", 2));
    assertEquals(19, provider.virtualThreadLimit("WORKFLOW_JOIN", 1000));
  }

  @Test
  void defaultsVirtualThreadsToDisabled() {
    ExecutionTuningProvider provider =
        new DefaultExecutionTuningProvider(
            new DefaultRatchetConfig(new EnvironmentRatchetConfigSource()));

    assertFalse(provider.useVirtualThreads());
  }
}
