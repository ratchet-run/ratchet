package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobTerminalStoreContract;

class MysqlJobTerminalStoreContractTest extends AbstractJobTerminalStoreContract {

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

  @Test
  void compareAndSwapStatus_cancelMissDoesNotTerminalizeCurrentStatus() {
    JobEntity saved = persist(newPendingJob());
    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));

    boolean canceled =
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);

    assertFalse(canceled, "stale expected status must not cancel a now-RUNNING job");
    assertEquals(JobStatus.RUNNING, store().findById(saved.getId()).orElseThrow().getStatus());
  }

  @Test
  void compareAndSwapStatus_cancelMatchesCurrentLiveStatusOnce() {
    JobEntity saved = persist(newPendingJob());
    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));

    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null));
    assertFalse(
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null));
    assertEquals(JobStatus.CANCELED, store().findById(saved.getId()).orElseThrow().getStatus());
  }
}
