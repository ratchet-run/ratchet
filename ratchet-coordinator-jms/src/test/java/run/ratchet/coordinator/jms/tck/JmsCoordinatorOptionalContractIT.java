package run.ratchet.coordinator.jms.tck;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import run.ratchet.coordinator.jms.EmbeddedArtemisBroker;
import run.ratchet.coordinator.jms.JmsCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorOptionalContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorOptionalContract} (pre-registration buffer) against an
 * embedded Artemis broker.
 */
class JmsCoordinatorOptionalContractIT extends AbstractClusterCoordinatorOptionalContract {

  private static EmbeddedArtemisBroker broker;

  @BeforeAll
  static void start() throws Exception {
    broker = new EmbeddedArtemisBroker();
    broker.start();
  }

  @AfterAll
  static void stop() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

  @Override
  protected CoordinatorTestHarness harness() {
    return new JmsCoordinatorTestHarness(broker);
  }
}
