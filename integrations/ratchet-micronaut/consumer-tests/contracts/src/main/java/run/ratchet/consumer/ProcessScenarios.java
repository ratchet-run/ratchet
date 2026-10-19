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
package run.ratchet.consumer;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class ProcessScenarios {
  private ProcessScenarios() {}

  public static Map<String, Object> options(Map<String, Object> connection) {
    Map<String, Object> options = new HashMap<>(connection);
    options.put("consumer.native-check", true);
    options.put("ratchet.schema.auto-migrate", true);
    options.put("ratchet.allowed-packages", "example.ratchet");
    options.put("ratchet.shutdown-timeout", "20s");
    options.put("ratchet.poller.min-delay-ms", 100);
    options.put("ratchet.poller.max-delay-ms", 200);
    options.put("ratchet.poller.deep-idle-delay-ms", 200);
    options.put("ratchet.node.heartbeat-interval-seconds", 1);
    options.put("ratchet.node.orphan-grace-seconds", 2);
    options.put("ratchet.node.orphan-scan-interval-seconds", 1);
    options.put("ratchet.node.dynamic-heartbeat-enabled", false);
    options.put("ratchet.recurring.poll-ms", 100);
    options.put("ratchet.recurring.max-poll-ms", 200);
    return options;
  }

  public static void checks(String app, Map<String, Object> options, String... additional)
      throws Exception {
    try (var process = ConsumerProcess.start(app, options, Path.of("target/native-checks.log"))) {
      require(process.awaitExit() == 0, process.output());
      for (String marker :
          new String[] {
            "METHOD_REFERENCE",
            "CAPTURING_LAMBDA",
            "STATIC_METHOD",
            "NESTED_SUBMITTER",
            "ANONYMOUS_SUBMITTER",
            "LOCAL_SUBMITTER",
            "INHERITED_SUBMITTER",
            "RECORD_ARGUMENT",
            "POJO_ARGUMENT",
            "RECURRING",
            "ALL_CHECKS"
          }) {
        require(process.output().contains("RATCHET_CHECK " + marker), process.output());
      }
      for (String marker : additional)
        require(process.output().contains("RATCHET_CHECK " + marker), process.output());
    }
  }

  public static String lifecycle(String app, Map<String, Object> options) throws Exception {
    String id;
    options.put("consumer.scenario", "drain");
    options.put("consumer.drain", true);
    try (var process = ConsumerProcess.start(app, options, Path.of("target/native-drain.log"))) {
      process.awaitMarker("RATCHET_JOB_STARTED durable");
      process.awaitMarker("RATCHET_JOB_ID ");
      id =
          process
              .output()
              .lines()
              .filter(line -> line.startsWith("RATCHET_JOB_ID "))
              .findFirst()
              .orElseThrow()
              .substring("RATCHET_JOB_ID ".length())
              .trim();
      long start = System.nanoTime();
      process.terminate();
      process.awaitExit(25);
      require(System.nanoTime() - start < 25_000_000_000L, process.output());
      require(process.output().contains("RATCHET_JOB_COMPLETED durable"), process.output());
    }
    // Micronaut ignores an empty override and keeps the packaged default, so pass a prefix the
    // class policy rejects instead.
    options.put("ratchet.allowed-packages", "x");
    try (var process =
        ConsumerProcess.start(app, options, Path.of("target/native-startup-failure.log"))) {
      require(process.awaitExit(30) != 0, process.output());
      require(
          process.output().contains("Allowed package prefix must be at least 3 characters"),
          process.output());
    }
    return id;
  }

  public static String recovery(String app, Map<String, Object> options) throws Exception {
    options.put("consumer.scenario", "block");
    options.put("consumer.block", true);
    options.put("ratchet.node.id", "native-recovery-first");
    String id;
    try (var process = ConsumerProcess.start(app, options, Path.of("target/recovery-first.log"))) {
      process.awaitMarker("RATCHET_JOB_STARTED durable");
      process.awaitMarker("RATCHET_JOB_ID ");
      id =
          process
              .output()
              .lines()
              .filter(line -> line.startsWith("RATCHET_JOB_ID "))
              .findFirst()
              .orElseThrow()
              .substring("RATCHET_JOB_ID ".length())
              .trim();
      process.kill();
    }
    options.put("consumer.block", false);
    options.put("consumer.scenario", "restart");
    options.put("consumer.job-id", id);
    options.put("ratchet.node.id", "native-recovery-second");
    try (var process = ConsumerProcess.start(app, options, Path.of("target/recovery-second.log"))) {
      require(process.awaitExit() == 0, process.output());
      require(process.output().contains("RATCHET_JOB_COMPLETED durable"), process.output());
      require(process.output().contains("RATCHET_CHECK RECOVERY"), process.output());
    }
    return id;
  }

  public static void require(boolean condition, String reason) {
    if (!condition) throw new AssertionError(reason);
  }
}
