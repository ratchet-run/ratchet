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
package run.ratchet.consumer.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static run.ratchet.consumer.chaos.UnitCrashChaosIT.assertCheckpointDone;
import static run.ratchet.consumer.chaos.UnitCrashChaosIT.assertPages;
import static run.ratchet.consumer.chaos.UnitCrashChaosIT.assertSettled;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class UnitStaleOwnerChaosIT {
  @Test
  void pausedOwnerUnitRollsBackAfterReclaim() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitUnits("unit-stale-owner", "unit-write", 0);
      var hit = cluster.gates.awaitHit("unit-write", job, a -> true, Duration.ofSeconds(30));
      assertThat(
              cluster.jdbc.queryForObject(
                  "select count(*) from chaos_unit_business where job_id = ?", Integer.class, job))
          .isZero();
      int owner = cluster.index(hit.incarnation());
      cluster.stop(owner);
      // The takeover records its attempt before its insert blocks on the frozen owner's key.
      await()
          .atMost(Duration.ofSeconds(60))
          .until(
              () ->
                  cluster.ledger().attempts().stream()
                      .anyMatch(
                          a ->
                              a.jobId().equals(job) && !a.incarnation().equals(hit.incarnation())));
      cluster.resume(owner);
      cluster.gates.release(hit.hitId());
      assertSettled(cluster, "unit-stale-owner", job);
      assertPages(cluster, job);
      assertThat(
              cluster.jdbc.queryForList(
                  "select incarnation from chaos_unit_business where job_id = ?",
                  String.class,
                  job))
          .doesNotContain(hit.incarnation());
      await()
          .atMost(Duration.ofSeconds(90))
          .untilAsserted(
              () ->
                  assertThat(cluster.ledger().attempts())
                      .anyMatch(
                          a ->
                              a.jobId().equals(job)
                                  && a.incarnation().equals(hit.incarnation())
                                  && a.finishedUs() != null
                                  && "superseded".equals(a.outcome())));
      assertCheckpointDone(cluster, job);
    }
  }
}
