package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobLogEntityTest {

  @Test
  void equalLogEntriesHaveSameHashCodeWhenIdsDiffer() {
    Instant timestamp = Instant.parse("2026-05-07T12:34:56Z");
    UUID jobId = UUID.fromString("0196b171-3f80-7000-8000-000000000001");

    JobLogEntity first =
        logEntry(UUID.fromString("0196b171-3f80-7000-8000-000000000101"), jobId, timestamp);
    JobLogEntity second =
        logEntry(UUID.fromString("0196b171-3f80-7000-8000-000000000202"), jobId, timestamp);

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

  @Test
  void logContentHasNoPublicSetters() {
    for (var method : JobLogEntity.class.getMethods()) {
      assertFalse(
          method.getName().matches("set(JobId|Ts|Level|Message|Mdc)"),
          () -> "Unexpected mutator remains: " + method);
    }
  }
}
