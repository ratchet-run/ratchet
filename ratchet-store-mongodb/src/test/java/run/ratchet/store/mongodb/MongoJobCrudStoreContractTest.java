package run.ratchet.store.mongodb;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobCrudStoreContract;

/** MongoDB contract test for {@code JobCrudStore} operations. */
class MongoJobCrudStoreContractTest extends AbstractJobCrudStoreContract {

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

  @Override
  public boolean supportsTransactionalRollback() {
    return fixture.supportsTransactionalRollback();
  }

  @Override
  public boolean isStaleWriteException(Throwable t) {
    return fixture.isStaleWriteException(t);
  }
}
