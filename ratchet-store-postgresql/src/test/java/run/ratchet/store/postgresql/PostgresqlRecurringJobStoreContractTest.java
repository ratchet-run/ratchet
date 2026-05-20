package run.ratchet.store.postgresql;

import java.util.List;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.tck.store.AbstractRecurringJobStoreContract;

class PostgresqlRecurringJobStoreContractTest extends AbstractRecurringJobStoreContract {

  private final PostgresqlTestFixture fixture = new PostgresqlTestFixture();

  @Override
  protected RecurringJobStore recurringStore() {
    return (RecurringJobStore) fixture.store();
  }

  @Override
  protected JobPayload noopPayload() {
    return new JobPayload("run.ratchet.tck.store.NoopTask", "run", "()V", true, List.of());
  }

  @Override
  protected void cleanupRecurringStore() {
    fixture.cleanupStore();
  }
}
