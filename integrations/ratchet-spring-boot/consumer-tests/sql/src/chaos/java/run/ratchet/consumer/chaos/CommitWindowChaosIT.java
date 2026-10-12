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

class CommitWindowChaosIT {
  @Test
  void recoversBeforeTerminalCommit() throws Exception {
    try (var cluster = new ChaosCluster()) {
      String job = cluster.submitGated("commit-window", 0, 0, "before-commit", 1);
      var hit = cluster.gates.awaitHit("before-commit", job, a -> true, Duration.ofSeconds(30));
      cluster.kill(cluster.index(hit.incarnation()));
      var violations = new ArrayList<>(cluster.settle(Duration.ofSeconds(90)));
      var ledger = cluster.ledger();
      var recovered =
          ledger.attempts().stream()
              .filter(a -> a.jobId().equals(job) && !a.incarnation().equals(hit.incarnation()))
              .toList();
      if (recovered.isEmpty()
          || ledger.commits().stream()
              .noneMatch(
                  c ->
                      c.jobId().equals(job)
                          && recovered.stream()
                              .anyMatch(a -> a.incarnation().equals(c.incarnation()))))
        violations.add(
            new Oracle.Violation(
                "commit-window", job, "No second incarnation ran and committed the recovered job"));
      RunReport.assertClean("commit-window", cluster.store(), cluster.timeline, ledger, violations);
    }
  }
}
