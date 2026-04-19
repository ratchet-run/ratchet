package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractBatchStoreContract;

class PostgresqlBatchStoreContractTest extends AbstractBatchStoreContract {

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
}
