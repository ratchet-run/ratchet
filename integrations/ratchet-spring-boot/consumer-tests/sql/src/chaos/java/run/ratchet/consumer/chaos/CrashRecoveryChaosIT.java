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
import java.util.Random;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CrashRecoveryChaosIT {
  @Test
  void recoversKilledOwner() throws Exception {
    long seed = 1101L;
    System.out.println("CrashRecoveryChaosIT seed=" + seed);
    var random = new Random(seed);
    try (var cluster = new ChaosCluster()) {
      // Submit alongside fault observation so short jobs cannot drain during HTTP submission.
      var producer =
          new FutureTask<Void>(
              () -> {
                for (int i = 0; i < 200; i++) cluster.submit("job-" + i, 300 + random.nextInt(501));
                return null;
              });
      Thread thread = new Thread(producer, "chaos-submitter");
      thread.start();
      try {
        int victim = cluster.busiest(3, Duration.ofSeconds(30));
        cluster.kill(victim);
        // Remaining submissions must avoid the killed endpoint.
        producer.get(60, TimeUnit.SECONDS);
        var violations = new ArrayList<>(cluster.settle(Duration.ofSeconds(90)));
        var ledger = cluster.ledger();
        boolean recovered =
            ledger.attempts().stream()
                .anyMatch(
                    a ->
                        a.nodeId().equals(cluster.nodeId(victim))
                            && a.finishedUs() == null
                            && ledger.attempts().stream()
                                .anyMatch(
                                    b ->
                                        b.jobId().equals(a.jobId())
                                            && !b.nodeId().equals(a.nodeId())
                                            && "ok".equals(b.outcome())));
        if (!recovered)
          violations.add(new Oracle.Violation("S1", null, "No job re-executed on another node"));
        RunReport.assertClean(
            "crash-recovery", cluster.store(), cluster.timeline, ledger, violations);
      } finally {
        thread.join(30000);
        if (thread.isAlive()) {
          thread.interrupt();
          thread.join(5000);
          if (thread.isAlive()) throw new AssertionError("Submitter did not stop");
        }
      }
    }
  }
}
