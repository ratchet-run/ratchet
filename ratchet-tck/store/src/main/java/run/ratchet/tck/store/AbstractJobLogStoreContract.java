/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobLogEntity;

/** Base contract tests for {@code JobLogStore}. */
public abstract class AbstractJobLogStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupLogFixture() {
    cleanupStore();
  }

  @Test
  void appendLog_persistsLogEntry() {
    var saved = persist(newPendingJob());

    JobLogEntity log =
        new JobLogEntity(
            saved.getId(), Instant.now(), JobLogEntity.LogLevel.INFO, "test log message");

    assertDoesNotThrow(() -> store().appendLog(log), "appendLog should not throw");
  }

  @Test
  void purgeLogsOlderThan_deletesOldLogs() {
    var saved = persist(newPendingJob());

    JobLogEntity log =
        new JobLogEntity(
            saved.getId(),
            Instant.now().minusSeconds(3600),
            JobLogEntity.LogLevel.INFO,
            "old log message");
    store().appendLog(log);

    int purged = store().purgeLogsOlderThan(Instant.now().minusSeconds(1800));

    assertEquals(1, purged, "purgeLogsOlderThan should delete the old log entry");
  }

  @Test
  void purgeLogsOlderThan_preservesRecentLogs() {
    var saved = persist(newPendingJob());

    JobLogEntity recentLog =
        new JobLogEntity(saved.getId(), Instant.now(), JobLogEntity.LogLevel.INFO, "recent log");
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
      JobLogEntity log =
          new JobLogEntity(saved.getId(), oldTime, JobLogEntity.LogLevel.INFO, "log entry " + i);
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

    JobLogEntity logA =
        new JobLogEntity(jobA.getId(), oldTime, JobLogEntity.LogLevel.INFO, "log for job A");
    store().appendLog(logA);

    JobLogEntity logB =
        new JobLogEntity(
            jobB.getId(), Instant.now(), JobLogEntity.LogLevel.INFO, "recent log for job B");
    store().appendLog(logB);

    int purged = store().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(1, purged, "Only the old log entry (job A) should be purged");
  }

  @Test
  void appendLog_withMdcMap_persistsContext() {
    var saved = persist(newPendingJob());

    JobLogEntity log =
        new JobLogEntity(
            saved.getId(),
            Instant.now(),
            JobLogEntity.LogLevel.INFO,
            "log with MDC",
            Map.of("traceId", "abc-123", "spanId", "def-456"));

    assertDoesNotThrow(() -> store().appendLog(log), "appendLog with MDC map should not throw");
  }

  @Test
  void appendLog_allLogLevels_persists() {
    var saved = persist(newPendingJob());

    for (JobLogEntity.LogLevel level : JobLogEntity.LogLevel.values()) {
      JobLogEntity log =
          new JobLogEntity(saved.getId(), Instant.now(), level, "test " + level.name());
      assertDoesNotThrow(
          () -> store().appendLog(log), "appendLog should accept " + level.name() + " level");
    }
  }
}
