package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractBatchStoreContract;

class MysqlBatchStoreContractTest extends AbstractBatchStoreContract {

  private final Fixture fixture = new Fixture();

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

  @Test
  void incrementCompletedAtomic_failsOnCorruptProgressHookPayload() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);
    fixture.corruptProgressHook(parent.getId());

    assertThrows(
        IllegalArgumentException.class, () -> store().incrementCompletedAtomic(parent.getId()));
  }

  private static final class Fixture extends MysqlTestFixture {
    void corruptProgressHook(UUID batchId) {
      String batchIdHex = batchId.toString().replace("-", "");
      executeNativeSql(
          "UPDATE scheduler_batch SET progress_hook = CAST('\"not-a-job-payload\"' AS JSON) "
              + "WHERE batch_id = UNHEX('"
              + batchIdHex
              + "')");
    }
  }
}
