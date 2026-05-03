package run.ratchet.store.mongodb;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobClaimStoreContract;
import org.junit.jupiter.api.BeforeEach;

/** MongoDB contract test for {@code JobClaimStore} operations. */
class MongoJobClaimStoreContractTest extends AbstractJobClaimStoreContract {

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

  @BeforeEach
  void cleanupBefore() {
    fixture.cleanupStore();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }
}
