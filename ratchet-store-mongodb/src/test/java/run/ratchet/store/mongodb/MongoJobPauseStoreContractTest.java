package run.ratchet.store.mongodb;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobPauseStoreContract;

/** MongoDB contract test for {@code JobPauseStore} operations. */
class MongoJobPauseStoreContractTest extends AbstractJobPauseStoreContract {

  private final MongoTestFixture fixture = new MongoTestFixture();

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
