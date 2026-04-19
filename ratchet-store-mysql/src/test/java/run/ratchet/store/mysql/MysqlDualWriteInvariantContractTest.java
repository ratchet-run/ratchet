package run.ratchet.store.mysql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractDualWriteInvariantContract;

/**
 * The critical contract — MySQL is the store with the actual hot/cold split, so this subclass is
 * the primary consumer of the shared invariants. Expect some tests to fail on first run (plan Phase
 * 2.1 Risk #1); each failure represents a real dual-write defect to fix, not a contract to relax.
 */
class MysqlDualWriteInvariantContractTest extends AbstractDualWriteInvariantContract {

  private final MysqlTestFixture fixture = new MysqlTestFixture();

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }
}
