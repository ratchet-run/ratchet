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
package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobLogEntityTest {

  @Test
  void distinctIdsAreNotEqualEvenWhenContentMatches() {
    // Distinct log rows must remain distinct in collections, even if they share content
    // (e.g., the same job emitting two INFO messages with identical text in the same tick).
    Instant timestamp = Instant.parse("2026-05-07T12:34:56Z");
    UUID jobId = UUID.fromString("0196b171-3f80-7000-8000-000000000001");

    JobLogEntity first =
        logEntry(UUID.fromString("0196b171-3f80-7000-8000-000000000101"), jobId, timestamp);
    JobLogEntity second =
        logEntry(UUID.fromString("0196b171-3f80-7000-8000-000000000202"), jobId, timestamp);

    assertNotEquals(first, second);
  }

  @Test
  void sameIdImpliesEqual() {
    UUID id = UUID.fromString("0196b171-3f80-7000-8000-000000000505");
    UUID jobId = UUID.fromString("0196b171-3f80-7000-8000-000000000001");
    JobLogEntity first = logEntry(id, jobId, Instant.parse("2026-05-07T12:34:56Z"));
    JobLogEntity second = logEntry(id, jobId, Instant.parse("2026-05-07T12:34:56Z"));

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  private JobLogEntity logEntry(UUID id, UUID jobId, Instant timestamp) {
    JobLogEntity log =
        new JobLogEntity(jobId, timestamp, JobLogEntity.LogLevel.INFO, "Job started");
    log.setId(id);
    return log;
  }

  @Test
  void mdcIsDefensivelyCopied() {
    Map<String, Object> mdc = new HashMap<>();
    mdc.put("traceId", "abc-123");
    JobLogEntity log =
        new JobLogEntity(
            UUID.fromString("0196b171-3f80-7000-8000-000000000001"),
            Instant.parse("2026-05-07T12:34:56Z"),
            JobLogEntity.LogLevel.INFO,
            "Job started",
            mdc);

    mdc.put("traceId", "mutated");

    assertEquals("abc-123", log.getMdc().get("traceId"));
    assertThrows(UnsupportedOperationException.class, () -> log.getMdc().put("spanId", "def-456"));
  }
}
