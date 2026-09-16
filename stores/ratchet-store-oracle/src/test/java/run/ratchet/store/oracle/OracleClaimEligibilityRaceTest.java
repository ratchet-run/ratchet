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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.oracle.converter.UuidRawConverter;

/** Real Oracle transactions, paused after the unlocked candidate SELECT has returned. */
class OracleClaimEligibilityRaceTest {
  private final OracleTestFixture fixture = new OracleTestFixture();

  @Test
  void committedRescheduleOrTagRemovalBetweenPhasesPreventsClaim() throws Exception {
    for (boolean optimized : List.of(false, true)) {
      for (boolean removeTag : List.of(false, true)) {
        fixture.cleanupStore();
        var job = fixture.newPendingJob();
        job.setTags(List.of("required"));
        job = fixture.store().create(job);
        var selected = new CountDownLatch(1);
        var mutationCommitted = new CountDownLatch(1);
        fixture.observeNativeQueries(
            (sql, event) -> {
              if (event.completed()
                  && event.operation().equals("getResultList")
                  && sql.contains("FROM scheduler_job_queue")
                  && sql.contains("FETCH FIRST")) {
                assertEquals(
                    1,
                    ((List<?>) event.result()).size(),
                    "phase A must actually select the candidate before mutation");
                selected.countDown();
                await(mutationCommitted);
              }
            });
        var executor = Executors.newSingleThreadExecutor();
        try {
          var claim =
              executor.submit(
                  () -> {
                    var filter = new NodeTagFilter(List.of("required"), List.of());
                    return optimized
                        ? fixture
                            .store()
                            .claimNextBatchOptimized(JobExecutionType.SINGLE, 1, "racer", filter)
                            .size()
                        : fixture.store().claimNextBatch(1, "racer", filter).size();
                  });
          assertTrue(
              selected.await(20, TimeUnit.SECONDS), "phase A must have read the due candidate");
          try (var connection = fixture.openConnection()) {
            connection.setAutoCommit(false);
            String sql =
                removeTag
                    ? "DELETE FROM scheduler_job_tag WHERE job_id = ?"
                    : "UPDATE scheduler_job_queue SET scheduled_time = ? WHERE job_id = ?";
            try (var update = connection.prepareStatement(sql)) {
              int parameter = 1;
              if (!removeTag)
                update.setTimestamp(parameter++, Timestamp.from(Instant.now().plusSeconds(3600)));
              update.setBytes(parameter, UuidRawConverter.toBytes(job.getId()));
              assertEquals(1, update.executeUpdate());
            }
            connection.commit();
          }
          mutationCommitted.countDown();
          assertEquals(
              0,
              claim.get(20, TimeUnit.SECONDS),
              "phase B must reject the now-ineligible candidate");
          var unchanged = fixture.store().findById(job.getId()).orElseThrow();
          assertEquals(JobStatus.PENDING, unchanged.getStatus());
          assertNull(unchanged.getPickedBy());
        } finally {
          mutationCommitted.countDown();
          executor.shutdownNow();
          assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
          fixture.observeNativeQueries((sql, event) -> {});
          fixture.cleanupStore();
        }
      }
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(
          latch.await(20, TimeUnit.SECONDS), "second transaction must commit before phase B");
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }
}
