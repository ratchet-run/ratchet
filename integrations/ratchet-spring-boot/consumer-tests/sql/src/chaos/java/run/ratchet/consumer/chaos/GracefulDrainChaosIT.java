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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GracefulDrainChaosIT {
  @Test
  void drainsTerminatedOwner() throws Exception {
    try (var cluster = new ChaosCluster()) {
      var producer =
          new FutureTask<Void>(
              () -> {
                for (int i = 0; i < 150; i++) cluster.submit("job-" + i, 600);
                return null;
              });
      Thread thread = new Thread(producer, "chaos-submitter");
      thread.start();
      try {
        cluster.terminate(cluster.busiest(1, Duration.ofSeconds(30)));
        producer.get(60, TimeUnit.SECONDS);
        var violations = cluster.settle(Duration.ofSeconds(90));
        RunReport.assertClean(
            "graceful-drain", cluster.store(), cluster.timeline, cluster.ledger(), violations);
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
