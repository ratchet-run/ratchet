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

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TransactionalStaleOwnerChaosIT {
  @Test
  void staleOwnerRollsBackBeforeTakeoverCommits() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitTransactional("TransactionalStaleOwnerChaosIT");
      var hit =
          cluster.gates.awaitHit("transactional-write", job, a -> true, Duration.ofSeconds(30));
      assertThat(
              cluster.jdbc.queryForObject(
                  "select count(*) from chaos_business where job_id = ?", Integer.class, job))
          .isZero();
      int owner = cluster.index(hit.incarnation());
      cluster.stop(owner);
      // Wait for a claim observation only: the takeover's business insert blocks on A's key.
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
      // Inline transient retries may produce extra attempt rows; do not count attempts.
      await()
          .atMost(Duration.ofSeconds(90))
          .untilAsserted(
              () -> {
                assertThat(cluster.probe.statuses(cluster.run).get(job)).isEqualTo("SUCCEEDED");
                var writers =
                    cluster.jdbc.queryForList(
                        "select incarnation from chaos_business where job_id = ?",
                        String.class,
                        job);
                assertThat(writers).hasSize(1);
                assertThat(writers.get(0)).isNotEqualTo(hit.incarnation());
                assertThat(cluster.ledger().attempts())
                    .anyMatch(a -> a.jobId().equals(job) && a.incarnation().equals(writers.get(0)));
                assertThat(
                        cluster.jdbc.queryForObject(
                            "select node_id from chaos_business where job_id = ?",
                            String.class,
                            job))
                    .isEqualTo(cluster.nodeId(cluster.index(writers.get(0))));
                assertThat(
                        cluster.ledger().commits().stream()
                            .filter(c -> c.jobId().equals(job))
                            .toList())
                    .singleElement()
                    .satisfies(
                        c -> {
                          assertThat(c.terminalStatus()).isEqualTo("SUCCEEDED");
                          assertThat(c.incarnation()).isEqualTo(writers.get(0));
                        });
              });
      await()
          .during(Duration.ofSeconds(2))
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(
                          cluster.ledger().commits().stream()
                              .filter(c -> c.jobId().equals(job))
                              .count())
                      .isEqualTo(1));
    }
  }
}
