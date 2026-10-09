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
class DrainTimeoutChaosIT {
  @Test
  void drainTimeoutChargesNothing() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitGated("drain-timeout", 0, 0, "body", 2);
      var held = cluster.gates.awaitHit("body", job, incarnation -> true, Duration.ofSeconds(60));
      int owner = cluster.index(held.incarnation());
      // The gate is never released, so the job outlives the drain and the worker is interrupted.
      cluster.terminate(owner);
      cluster.restart(owner);
      var redelivered =
          cluster.gates.awaitHit(
              "body",
              job,
              incarnation -> !incarnation.equals(held.incarnation()),
              Duration.ofSeconds(60));
      // The drain charged nothing, so one crash still fits in a budget of one.
      int next = cluster.index(redelivered.incarnation());
      cluster.kill(next);
      cluster.restart(next);
      var violations = new ArrayList<>(cluster.settle(Duration.ofSeconds(90)));
      String status = cluster.probe.statuses(cluster.run).get(job);
      if (!"SUCCEEDED".equals(status))
        violations.add(
            new Oracle.Violation(
                "drain-timeout",
                job,
                "A drain timeout and one crash must not exhaust maxRetries=0 and a crash budget"
                    + " of 1; observed "
                    + status));
      RunReport.assertClean(
          "drain-timeout", cluster.store(), cluster.timeline, cluster.ledger(), violations);
    }
  }
}
