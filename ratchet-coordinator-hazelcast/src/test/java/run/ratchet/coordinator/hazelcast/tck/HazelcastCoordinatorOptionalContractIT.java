package run.ratchet.coordinator.hazelcast.tck;

import run.ratchet.coordinator.hazelcast.HazelcastCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorOptionalContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorOptionalContract} (pre-registration buffer) against two
 * embedded Hazelcast members.
 */
class HazelcastCoordinatorOptionalContractIT extends AbstractClusterCoordinatorOptionalContract {

  @Override
  protected CoordinatorTestHarness harness() {
    return new HazelcastCoordinatorTestHarness();
  }
}
