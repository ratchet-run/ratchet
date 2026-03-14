package run.ratchet.store.mongodb;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobStatusStoreContract;

/** MongoDB contract test for {@code JobStatusStore} operations. */
class MongoJobStatusStoreContractTest extends AbstractJobStatusStoreContract {

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
