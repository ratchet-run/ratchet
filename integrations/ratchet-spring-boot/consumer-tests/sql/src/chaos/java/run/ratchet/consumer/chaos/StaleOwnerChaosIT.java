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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StaleOwnerChaosIT {
  @Test
  void staleIncarnationCannotCommitNewOwnersJob() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitGated("stale-owner", 0, 0, "body", 2);
      var a = cluster.gates.awaitHit("body", job, incarnation -> true, Duration.ofSeconds(30));
      int owner = cluster.index(a.incarnation());
      cluster.stop(owner);
      // B reaching its body proves orphan recovery reset and redelivered the same job.
      var b =
          cluster.gates.awaitHit(
              "body",
              job,
              incarnation -> !incarnation.equals(a.incarnation()),
              Duration.ofSeconds(60));
      cluster.resume(owner);
      cluster.gates.release(a.hitId());
      long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (System.nanoTime() < deadline
          && cluster.ledger().commits().stream().noneMatch(c -> c.jobId().equals(job)))
        Thread.sleep(50);
      // A correct fence leaves B free to commit after this bounded observation window.
      cluster.gates.release(b.hitId());
      var violations = cluster.settle(Duration.ofSeconds(90));
      RunReport.assertClean(
          "stale-owner", cluster.store(), cluster.timeline, cluster.ledger(), violations);
      var attempts =
          cluster.ledger().attempts().stream()
              .filter(attempt -> attempt.jobId().equals(job))
              .toList();
      var stale =
          attempts.stream()
              .filter(attempt -> attempt.incarnation().equals(a.incarnation()))
              .toList();
      var current =
          attempts.stream()
              .filter(attempt -> attempt.incarnation().equals(b.incarnation()))
              .toList();
      assertEquals(1, stale.size(), "Expected one attempt from the stopped owner");
      assertEquals(1, current.size(), "Expected one attempt from the new owner");
      assertEquals(
          "superseded", stale.get(0).checkpointResult(), "Stale checkpoint must be fenced");
      assertEquals("ok", current.get(0).checkpointResult(), "Current checkpoint must be stored");
      assertEquals("false", stale.get(0).currentAttempt(), "Stale owner must observe supersession");
      assertEquals(
          "true", current.get(0).currentAttempt(), "New owner must observe its live claim");
    }
  }
}
