package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobLogEntity;

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

  @Test
  void purgeLogsOlderThan_preservesRecentLogs() {
    var saved = persist(newPendingJob());

    JobLogEntity recentLog = new JobLogEntity();
    recentLog.setJobId(saved.getId());
    recentLog.setTs(Instant.now());
    recentLog.setLevel(JobLogEntity.LogLevel.INFO);
    recentLog.setMessage("recent log");
    store().appendLog(recentLog);

    int purged = store().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(0, purged, "purgeLogsOlderThan should not delete recent logs");
  }

  @Test
  void purgeLogsOlderThan_emptyStore_returnsZero() {
    int purged = store().purgeLogsOlderThan(Instant.now());

    assertEquals(0, purged, "purgeLogsOlderThan on empty store should return 0");
  }

  @Test
  void appendLog_multipleEntries_allPersisted() {
    var saved = persist(newPendingJob());
    Instant oldTime = Instant.now().minusSeconds(7200);

    for (int i = 0; i < 3; i++) {
      JobLogEntity log = new JobLogEntity();
      log.setJobId(saved.getId());
      log.setTs(oldTime);
      log.setLevel(JobLogEntity.LogLevel.INFO);
      log.setMessage("log entry " + i);
      store().appendLog(log);
    }

    int purged = store().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(3, purged, "All 3 old log entries should be purged");
  }

  @Test
  void appendLog_differentJobs_isolatedByJobId() {
    var jobA = persist(newPendingJob());
    var jobB = persist(newPendingJob());
    Instant oldTime = Instant.now().minusSeconds(7200);

    JobLogEntity logA = new JobLogEntity();
    logA.setJobId(jobA.getId());
    logA.setTs(oldTime);
    logA.setLevel(JobLogEntity.LogLevel.INFO);
    logA.setMessage("log for job A");
    store().appendLog(logA);

    JobLogEntity logB = new JobLogEntity();
    logB.setJobId(jobB.getId());
    logB.setTs(Instant.now());
    logB.setLevel(JobLogEntity.LogLevel.INFO);
    logB.setMessage("recent log for job B");
    store().appendLog(logB);

    int purged = store().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(1, purged, "Only the old log entry (job A) should be purged");
  }

  @Test
  void appendLog_withMdcMap_persistsContext() {
    var saved = persist(newPendingJob());

    JobLogEntity log = new JobLogEntity();
    log.setJobId(saved.getId());
    log.setTs(Instant.now());
    log.setLevel(JobLogEntity.LogLevel.INFO);
    log.setMessage("log with MDC");
    log.setMdc(Map.of("traceId", "abc-123", "spanId", "def-456"));

    assertDoesNotThrow(() -> store().appendLog(log), "appendLog with MDC map should not throw");
  }

  @Test
  void appendLog_allLogLevels_persists() {
    var saved = persist(newPendingJob());

    for (JobLogEntity.LogLevel level : JobLogEntity.LogLevel.values()) {
      JobLogEntity log = new JobLogEntity();
      log.setJobId(saved.getId());
      log.setTs(Instant.now());
      log.setLevel(level);
      log.setMessage("test " + level.name());
      assertDoesNotThrow(
          () -> store().appendLog(log), "appendLog should accept " + level.name() + " level");
    }
  }
}
