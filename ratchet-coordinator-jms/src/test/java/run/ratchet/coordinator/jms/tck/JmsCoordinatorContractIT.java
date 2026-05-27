package run.ratchet.coordinator.jms.tck;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import run.ratchet.coordinator.jms.EmbeddedArtemisBroker;
import run.ratchet.coordinator.jms.JmsCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorContract} against an embedded Artemis broker.
 *
 * <p>One broker is shared across the class; each test provisions a fresh harness with a unique
 * topic name so concurrent tests do not cross-talk.
 */
class JmsCoordinatorContractIT extends AbstractClusterCoordinatorContract {

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
