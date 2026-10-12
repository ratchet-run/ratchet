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
import org.junit.jupiter.api.Test;

class ClaimedNotStartedChaosIT {
  @Test
  void recoversClaimWithoutStartingBody() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitGated("claimed-not-started", 0, 0, "after-claim", 1);
      var hit = cluster.gates.awaitHit("after-claim", job, a -> true, Duration.ofSeconds(30));
      cluster.kill(cluster.index(hit.incarnation()));
      var violations = new ArrayList<>(cluster.settle(Duration.ofSeconds(90)));
      var ledger = cluster.ledger();
      if (ledger.attempts().stream()
          .anyMatch(a -> a.jobId().equals(job) && a.incarnation().equals(hit.incarnation())))
        violations.add(
            new Oracle.Violation("claimed-not-started", job, "Killed claimant started a body"));
      RunReport.assertClean(
          "claimed-not-started", cluster.store(), cluster.timeline, ledger, violations);
    }
  }
}
