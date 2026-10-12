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
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RetryAccountingChaosIT {
  @Test
  void accountsForApplicationFailuresAcrossRecovery() throws Exception {
    long seed = Long.getLong("chaos.seed", System.nanoTime());
    System.out.println("RetryAccountingChaosIT seed=" + seed);
    var mix = new ArrayList<Oracle.JobKind>();
    // Include every combination before shuffling, ensuring both recovery and exhaustion coverage.
    for (int i = 0; i < 40; i++) mix.add(new Oracle.JobKind(i % 4, (i / 4) % 4));
    Collections.shuffle(mix, new Random(seed));
    try (var cluster = new ChaosCluster()) {
      var producer =
          new FutureTask<Void>(
              () -> {
                for (int i = 0; i < mix.size(); i++) {
                  var kind = mix.get(i);
                  cluster.submit("retry-" + i, 600, kind.failTimes(), kind.maxRetries());
                }
                return null;
              });
      Thread thread = new Thread(producer, "chaos-submitter");
      thread.start();
      try {
        cluster.kill(cluster.busiest(3, Duration.ofSeconds(30)));
        producer.get(60, TimeUnit.SECONDS);
        var violations = cluster.settle(Duration.ofSeconds(120));
        RunReport.assertClean(
            "retry-accounting", cluster.store(), cluster.timeline, cluster.ledger(), violations);
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
