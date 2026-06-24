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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

/**
 * Regression for the cancel-by-tag / claim race on SQL Server.
 *
 * <p>{@code cancelJobsByTag} marks the cold {@code scheduler_job} row CANCELED and then deletes the
 * hot {@code scheduler_job_queue} row. SQL Server's {@code UPDATE ... FROM} does not lock the
 * FROM-referenced queue row, so before the fix a poller could claim the tagged job into RUNNING in
 * the window between the cold update and the hot delete. The hot delete only matched
 * PENDING/PAUSED/WAITING, so it skipped the now-RUNNING row, stranding it forever — while the
 * reservation delete still freed the business key, allowing a duplicate to run.
 *
 * <p>The window is internal to the cancel method, so this races a real {@code cancelJobsByTag}
 * against a real {@code claimNextBatchOptimized} on a shared barrier across many repetitions. The
 * fix locks the candidate queue row with {@code WITH (UPDLOCK, ROWLOCK)} as the cancel's first
 * statement, so the claim's {@code WITH (UPDLOCK, READPAST, ROWLOCK)} skips it via READPAST. The
 * post-invariant after each round is exact: either the cancel won (job CANCELED, zero queue rows,
 * zero reservations) or the claim won (job still PENDING-or-RUNNING, exactly one queue row,
 * reservation intact). The broken interleave produced the forbidden mix — a CANCELED cold row with
 * a surviving RUNNING queue row and a freed reservation — which trips the assertions below.
 */
class SqlserverCancelByTagClaimConcurrencyTest {

  private static final long TIMEOUT_SECONDS = 20;
  private static final String TAG = "cancel-race-tag";

  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture.cleanupStore();
  }

  @RepeatedTest(40)
  void cancelAndClaimNeverStrandARunningRow() throws Exception {
    JobStore store = fixture.store();

    AtomicReference<UUID> jobIdRef = new AtomicReference<>();
    JobEntity job = fixture.newPendingJob();
    job.setBusinessKey("cancel-race-key");
    fixture.runInTransaction(
        () -> {
          UUID id = store.create(job).getId();
          store.insertTags(id, List.of(TAG));
          jobIdRef.set(id);
        });
    UUID jobId = jobIdRef.get();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier start = new CyclicBarrier(2);
    try {
      Future<?> cancelFuture =
          executor.submit(
              () -> {
                awaitBarrier(start);
                store.cancelJobsByTag(TAG);
                return null;
              });
      Future<?> claimFuture =
          executor.submit(
              () -> {
                awaitBarrier(start);
                store.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "poller-node");
                return null;
              });
      claimFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      cancelFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      assertConsistentOutcome(jobId);
    } catch (ExecutionException e) {
      throw new IllegalStateException("worker threw asynchronously", e.getCause());
    } finally {
      executor.shutdown();
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  /**
   * After cancel and claim settle, the job must be in exactly one of two consistent states. The
   * forbidden state the bug produced — cold row CANCELED, a surviving RUNNING queue row,
   * reservation freed — fails here.
   */
  private void assertConsistentOutcome(UUID jobId) throws Exception {
    try (Connection conn = fixture.openConnection()) {
      String terminal = terminalStatus(conn, jobId);
      String queueStatus = queueStatus(conn, jobId);
      long reservations = reservationCount(conn, jobId);

      if ("CANCELED".equals(terminal)) {
        assertEquals(
            null,
            queueStatus,
            "cancel won, so the hot queue row must be gone — a surviving "
                + queueStatus
                + " row is the stranded-orphan bug");
        assertEquals(0, reservations, "cancel won, so the reservation must be freed");
      } else {
        assertEquals(null, terminal, "claim won, so the cold row must not be terminal");
        // claim transitioned PENDING -> RUNNING (or it lost the claim and the row stays PENDING)
        if (queueStatus == null) {
          throw new AssertionError("claim won but the queue row vanished");
        }
        assertEquals(1, reservations, "claim won, so the reservation must remain held");
      }
    }
  }

  private String terminalStatus(Connection conn, UUID jobId) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT terminal_status FROM scheduler_job WHERE job_id = ?")) {
      ps.setBytes(1, UuidByteArrayConverter.toBytes(jobId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private String queueStatus(Connection conn, UUID jobId) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT status FROM scheduler_job_queue WHERE job_id = ?")) {
      ps.setBytes(1, UuidByteArrayConverter.toBytes(jobId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private long reservationCount(Connection conn, UUID jobId) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT COUNT(*) FROM scheduler_business_key_reservation WHERE owner_job_id = ?")) {
      ps.setBytes(1, UuidByteArrayConverter.toBytes(jobId));
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static void awaitBarrier(CyclicBarrier barrier) {
    try {
      barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException("barrier wait failed", e);
    }
  }
}
