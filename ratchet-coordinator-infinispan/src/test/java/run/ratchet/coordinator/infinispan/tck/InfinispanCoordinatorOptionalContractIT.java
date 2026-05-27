package run.ratchet.coordinator.infinispan.tck;

import run.ratchet.coordinator.infinispan.InfinispanCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorOptionalContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorOptionalContract} (pre-registration buffer) against two
 * embedded Infinispan cache managers.
 */
class InfinispanCoordinatorOptionalContractIT extends AbstractClusterCoordinatorOptionalContract {

  @Override
  protected CoordinatorTestHarness harness() {
    try {
      return new InfinispanCoordinatorTestHarness();
    } catch (Exception e) {
      throw new RuntimeException("could not bootstrap Infinispan harness", e);
    }
  }
}
