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
import java.util.Map;
import org.junit.jupiter.api.Test;

class DurableTerminalEventChaosIT {
  @Test
  void redeliversAfterTerminalCommitPublisherIsKilled() throws Exception {
    try (var cluster = new ChaosCluster(3, Map.of("ratchet.events.durable", true))) {
      String job = cluster.submitGated("lost-event", 0, 0, "after-commit", 1);
      var hit = cluster.gates.awaitHit("after-commit", job, a -> true, Duration.ofSeconds(30));
      cluster.kill(cluster.index(hit.incarnation()));
      var violations = new ArrayList<>(cluster.settle(Duration.ofSeconds(90)));
      // The relay on a surviving node redelivers the event, so the commit row names that node
      // rather than the attempt's. The check below asserts exactly that, so I8's committer rule
      // does not apply to this job. Its "multiple commits" rule still does.
      violations.removeIf(
          v ->
              "I8".equals(v.invariant())
                  && job.equals(v.jobId())
                  && v.evidence().startsWith("committer differs"));
      var ledger = cluster.ledger();
      var attempts = ledger.attempts().stream().filter(a -> a.jobId().equals(job)).toList();
      if (!"SUCCEEDED".equals(cluster.probe.statuses(cluster.run).get(job))
          || attempts.size() != 1
          || !"ok".equals(attempts.get(0).outcome())
          || ledger.commits().stream()
              .noneMatch(c -> c.jobId().equals(job) && !c.incarnation().equals(hit.incarnation())))
        violations.add(
            new Oracle.Violation(
                "lost-event",
                job,
                "Expected a succeeded job with one ok attempt and a commit row from a surviving"
                    + " incarnation"));
      RunReport.assertClean("lost-event", cluster.store(), cluster.timeline, ledger, violations);
    }
  }
}
