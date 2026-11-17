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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class UnitCrashChaosIT {
  @Test
  void killAfterUnitCommitResumesAtNextUnit() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitUnits("unit-commit-crash", "unit-between", 0);
      var hit = cluster.gates.awaitHit("unit-between", job, a -> true, Duration.ofSeconds(30));
      assertThat(writer(cluster, job, 0)).isEqualTo(hit.incarnation());
      cluster.kill(cluster.index(hit.incarnation()));
      assertSettled(cluster, "unit-commit-crash", job);
      assertPages(cluster, job);
      assertThat(writer(cluster, job, 0)).isEqualTo(hit.incarnation());
      String survivor = writer(cluster, job, 1);
      assertThat(survivor).isNotEqualTo(hit.incarnation());
      assertThat(writer(cluster, job, 2)).isEqualTo(survivor);
      assertStart(cluster, job, survivor, "1");
    }
  }

  @Test
  void orphanResetDuringUnitRollsBackUnit() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitUnits("unit-open-crash", "unit-write", 0);
      var hit = cluster.gates.awaitHit("unit-write", job, a -> true, Duration.ofSeconds(30));
      assertThat(
              cluster.jdbc.queryForObject(
                  "select count(*) from chaos_unit_business where job_id = ?", Integer.class, job))
          .isZero();
      cluster.kill(cluster.index(hit.incarnation()));
      assertSettled(cluster, "unit-open-crash", job);
      assertPages(cluster, job);
      assertThat(
              cluster.jdbc.queryForList(
                  "select incarnation from chaos_unit_business where job_id = ?",
                  String.class,
                  job))
          .doesNotContain(hit.incarnation());
      for (int page = 0; page < 3; page++)
        assertStart(cluster, job, writer(cluster, job, page), null);
    }
  }

  static void assertSettled(ChaosCluster cluster, String scenario, String job) throws Exception {
    var violations = cluster.settle(Duration.ofSeconds(90));
    RunReport.assertClean(
        scenario, cluster.store(), cluster.timeline, cluster.ledger(), violations);
    assertThat(cluster.probe.statuses(cluster.run).get(job)).isEqualTo("SUCCEEDED");
  }

  static void assertPages(ChaosCluster cluster, String job) {
    assertThat(
            cluster.jdbc.queryForList(
                "select page from chaos_unit_business where job_id = ? order by page",
                Integer.class,
                job))
        .containsExactly(0, 1, 2);
  }

  static String writer(ChaosCluster cluster, String job, int page) {
    return cluster.jdbc.queryForObject(
        "select incarnation from chaos_unit_business where job_id = ? and page = ?",
        String.class,
        job,
        page);
  }

  static void assertStart(ChaosCluster cluster, String job, String incarnation, String checkpoint) {
    // Retries can add observations. Check the first unit of the surviving execution.
    assertThat(
            cluster.jdbc.queryForList(
                """
                select start_checkpoint from chaos_unit_start
                where job_id = ? and incarnation = ? order by observed_us
                """,
                String.class,
                job,
                incarnation))
        .contains(checkpoint);
  }

  static void assertCheckpointDone(ChaosCluster cluster, String job) throws Exception {
    String id =
        cluster.store().equals("mysql") ? "BIN_TO_UUID(job_id)" : "cast(job_id as varchar(36))";
    String state =
        cluster.jdbc.queryForObject(
            "select state from scheduler_job_extension_state where "
                + id
                + " = ? and namespace = 'ratchet.checkpoint'",
            String.class,
            job);
    assertThat(new ObjectMapper().readTree(state).get("value").asText()).isEqualTo("done");
  }
}
