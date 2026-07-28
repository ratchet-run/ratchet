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
package run.ratchet.quarkus.runtime.devui;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import run.ratchet.api.QueueHealthSnapshot;

public record QueueHealthRow(
    long pendingCount,
    long runningCount,
    long failedCount,
    long succeededCount,
    long canceledCount,
    long pausedCount,
    long waitingCount,
    long stuckCount,
    long readyCount,
    double retryRate,
    double avgProcessingTimeMs,
    long p95QueueWaitMs,
    String oldestPendingJobTime,
    Map<String, Long> pendingByType,
    Map<String, Long> pendingByPriority) {

  public QueueHealthRow {
    pendingByType = pendingByType == null ? Map.of() : Map.copyOf(pendingByType);
    pendingByPriority = pendingByPriority == null ? Map.of() : Map.copyOf(pendingByPriority);
  }

  static QueueHealthRow from(QueueHealthSnapshot snapshot) {
    return new QueueHealthRow(
        snapshot.pendingCount(),
        snapshot.runningCount(),
        snapshot.failedCount(),
        snapshot.succeededCount(),
        snapshot.canceledCount(),
        snapshot.pausedCount(),
        snapshot.waitingCount(),
        snapshot.stuckCount(),
        snapshot.readyCount(),
        snapshot.retryRate(),
        snapshot.avgProcessingTimeMs(),
        snapshot.p95QueueWaitMs(),
        instantToString(snapshot.oldestPendingJobTime()),
        stringifyKeys(snapshot.pendingByType()),
        stringifyKeys(snapshot.pendingByPriority()));
  }

  static QueueHealthRow empty() {
    return new QueueHealthRow(0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0, null, Map.of(), Map.of());
  }

  private static String instantToString(Instant instant) {
    return instant == null ? null : instant.toString();
  }

  private static Map<String, Long> stringifyKeys(Map<?, Long> values) {
    if (values == null || values.isEmpty()) {
      return Map.of();
    }
    Map<String, Long> keyed = new LinkedHashMap<>();
    values.forEach((key, value) -> keyed.put(String.valueOf(key), value));
    return keyed;
  }
}
