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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.RuntimeProcess;

class DuplicateSubmitChaosIT {
  @Test
  void reservesEachIdempotencyKeyOnce() throws Exception {
    try (var cluster = new ChaosCluster()) {
      var pool = Executors.newFixedThreadPool(3);
      var violations = new ArrayList<Oracle.Violation>();
      try {
        for (int key = 0; key < 50; key++) {
          String jobKey = "key-" + key;
          String idempotency = UUID.randomUUID().toString();
          var barrier = new CyclicBarrier(3);
          var calls = new ArrayList<Callable<String>>();
          for (int i = 0; i < 3; i++) {
            int node = i;
            calls.add(
                () -> {
                  barrier.await(10, TimeUnit.SECONDS);
                  try {
                    return cluster.submit(node, jobKey, 300, idempotency, "");
                  } catch (RuntimeProcess.HttpFailure conflict) {
                    if (conflict.status != 409) throw conflict;
                    return null;
                  }
                });
          }
          var successes = new ArrayList<String>();
          for (var result : pool.invokeAll(calls)) {
            String id = result.get();
            if (id != null) successes.add(id);
          }
          if (successes.size() != 1)
            violations.add(
                new Oracle.Violation(
                    "S11", null, jobKey + " successful submissions: " + successes));
        }
      } finally {
        pool.shutdownNow();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS))
          throw new AssertionError("Submit pool did not stop");
      }
      violations.addAll(
          cluster.settle(Duration.ofSeconds(90)).stream()
              .filter(v -> List.of("I1", "I2", "I9").contains(v.invariant()))
              .toList());
      var statuses = cluster.probe.statuses(cluster.run);
      if (statuses.size() != 50)
        violations.add(new Oracle.Violation("S11", null, "run job count: " + statuses.size()));
      var ledger = cluster.ledger();
      for (int key = 0; key < 50; key++) {
        String jobKey = "key-" + key;
        var jobs =
            ledger.attempts().stream()
                .filter(a -> a.key().equals(jobKey))
                .map(Ledger.Attempt::jobId)
                .distinct()
                .toList();
        if (jobs.size() != 1 || !"SUCCEEDED".equals(statuses.get(jobs.get(0))))
          violations.add(
              new Oracle.Violation(
                  "S11",
                  jobs.isEmpty() ? null : jobs.get(0),
                  jobKey + " must have exactly one SUCCEEDED job; observed " + jobs));
      }
      RunReport.assertClean(
          "duplicate-submit", cluster.store(), cluster.timeline, ledger, violations);
    }
  }
}
