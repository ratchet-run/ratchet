package run.ratchet.coordinator.hazelcast.tck;

import run.ratchet.coordinator.hazelcast.HazelcastCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/** Runs the {@link AbstractClusterCoordinatorContract} against two embedded Hazelcast members. */
class HazelcastCoordinatorContractIT extends AbstractClusterCoordinatorContract {

  @Override
  protected CoordinatorTestHarness harness() {
    return new HazelcastCoordinatorTestHarness();
  }
}
