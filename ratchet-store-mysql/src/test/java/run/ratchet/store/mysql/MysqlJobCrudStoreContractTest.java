package run.ratchet.store.mysql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobCrudStoreContract;

class MysqlJobCrudStoreContractTest extends AbstractJobCrudStoreContract {

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

  @Override
  public boolean supportsTransactionalRollback() {
    return fixture.supportsTransactionalRollback();
  }

  @Override
  public boolean isStaleWriteException(Throwable t) {
    return fixture.isStaleWriteException(t);
  }
}
