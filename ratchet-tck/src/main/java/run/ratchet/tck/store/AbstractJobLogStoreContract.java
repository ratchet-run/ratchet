package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.store.entity.JobLogEntity;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobLogStore}. */
public abstract class AbstractJobLogStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupLogFixture() {
    cleanupStore();
  }

  @Test
  void appendLog_persistsLogEntry() {
    var saved = persist(newPendingJob());

    JobLogEntity log = new JobLogEntity();
    log.setJobId(saved.getId());
    log.setTs(Instant.now());
    log.setLevel(JobLogEntity.LogLevel.INFO);
    log.setMessage("test log message");

    assertDoesNotThrow(() -> store().appendLog(log), "appendLog should not throw");
  }

  @Test
  void purgeLogsOlderThan_deletesOldLogs() {
    var saved = persist(newPendingJob());

    JobLogEntity log = new JobLogEntity();
    log.setJobId(saved.getId());
    log.setTs(Instant.now().minusSeconds(3600));
    log.setLevel(JobLogEntity.LogLevel.INFO);
    log.setMessage("old log message");
    store().appendLog(log);

    int purged = store().purgeLogsOlderThan(Instant.now().minusSeconds(1800));

    assertEquals(1, purged, "purgeLogsOlderThan should delete the old log entry");
  }
}
