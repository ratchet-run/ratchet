package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobCrudStoreContract;

class PostgresqlJobCrudStoreContractTest extends AbstractJobCrudStoreContract {

  private final PostgresqlTestFixture fixture = new PostgresqlTestFixture();

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
