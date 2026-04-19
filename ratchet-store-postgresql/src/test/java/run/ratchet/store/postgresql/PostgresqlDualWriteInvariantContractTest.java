package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractDualWriteInvariantContract;

/**
 * PostgreSQL must still honor the active
 * business-key invariants. Running the contract here catches any regression when Phase 4.4 unifies
 * the bkres model across stores.
 */
class PostgresqlDualWriteInvariantContractTest extends AbstractDualWriteInvariantContract {

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
