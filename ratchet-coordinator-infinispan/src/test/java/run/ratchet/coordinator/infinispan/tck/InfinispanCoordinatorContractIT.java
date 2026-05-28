package run.ratchet.coordinator.infinispan.tck;

import run.ratchet.coordinator.infinispan.InfinispanCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorContract} against two embedded Infinispan cache
 * managers joined via JGroups TCP loopback.
 */
class InfinispanCoordinatorContractIT extends AbstractClusterCoordinatorContract {

  @Override
  protected CoordinatorTestHarness harness() {
    try {
      return new InfinispanCoordinatorTestHarness();
    } catch (Exception e) {
      throw new RuntimeException("could not bootstrap Infinispan harness", e);
    }
  }
}
