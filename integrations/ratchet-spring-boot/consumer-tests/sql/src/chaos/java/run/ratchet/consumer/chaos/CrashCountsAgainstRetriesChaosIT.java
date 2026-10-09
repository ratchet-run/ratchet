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

import java.time.Duration;
import java.util.ArrayList;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("known-defect")
class CrashCountsAgainstRetriesChaosIT {
  @Test
  void twoCrashesExhaustOneRetry() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitGated("crash-budget", 0, 1, "body", 2);
      String previous = "";
      for (int crash = 0; crash < 2; crash++) {
        String old = previous;
        var hit =
            cluster.gates.awaitHit(
                "body", job, incarnation -> !incarnation.equals(old), Duration.ofSeconds(60));
        int owner = cluster.index(hit.incarnation());
        cluster.kill(owner);
        cluster.restart(owner);
        previous = hit.incarnation();
      }
      // This regression specifies future crash accounting, rather than today's application-only
      // I10 rule. Retain delivery/fencing/cleanup checks while asserting the crash budget below.
      var violations =
          new ArrayList<>(
              cluster.settle(Duration.ofSeconds(90)).stream()
                  .filter(v -> !v.invariant().equals("I10"))
                  .toList());
      String status = cluster.probe.statuses(cluster.run).get(job);
      if (!"FAILED".equals(status))
        violations.add(
            new Oracle.Violation(
                "crash-retry-budget",
                job,
                "Two crashes with maxRetries=1 must dead-letter; observed " + status));
      RunReport.assertClean(
          "crash-retry-budget", cluster.store(), cluster.timeline, cluster.ledger(), violations);
    }
  }
}
