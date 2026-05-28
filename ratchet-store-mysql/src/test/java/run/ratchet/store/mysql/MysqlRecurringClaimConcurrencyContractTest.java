package run.ratchet.store.mysql;

import java.util.List;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.tck.store.AbstractJpaRecurringClaimConcurrencyContract;
import run.ratchet.tck.store.JpaContainerFixture;

class MysqlRecurringClaimConcurrencyContractTest
    extends AbstractJpaRecurringClaimConcurrencyContract {

  private final MysqlTestFixture fixture = new MysqlTestFixture();

  @Override
  protected JpaContainerFixture fixture() {
    return fixture;
  }

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
