package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StandaloneExecutorProviderTest {

  @Test
  void shutdownStopsOwnedExecutors() {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();

    assertFalse(provider.getJobExecutor().isShutdown());
    assertFalse(provider.getScheduledExecutor().isShutdown());

    provider.shutdown();

    assertTrue(provider.getJobExecutor().isShutdown());
    assertTrue(provider.getScheduledExecutor().isShutdown());
  }
}
