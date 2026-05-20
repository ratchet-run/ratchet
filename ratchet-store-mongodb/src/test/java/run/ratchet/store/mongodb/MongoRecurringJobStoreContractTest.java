package run.ratchet.store.mongodb;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.tck.store.AbstractRecurringJobStoreContract;

class MongoRecurringJobStoreContractTest extends AbstractRecurringJobStoreContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @Override
  protected RecurringJobStore recurringStore() {
    return (RecurringJobStore) fixture.store();
  }

  @Override
  protected TagStore tagStore() {
    return (TagStore) fixture.store();
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
