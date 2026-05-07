package run.ratchet.store.mongodb;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractDualWriteInvariantContract;

/**
 * MongoDB contract test for the dual-write lifecycle invariants. MongoDB stores everything in a
 * single collection, so the test exercises the equivalent observable behavior — terminal
 * transitions release business-key ownership, terminal jobs are unclaimable, and counts stay
 * coherent.
 */
class MongoDualWriteInvariantContractTest extends AbstractDualWriteInvariantContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

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
