package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobLogEntityTest {

  @Test
  void equalLogEntriesHaveSameHashCodeWhenIdsDiffer() {
    Instant timestamp = Instant.parse("2026-05-07T12:34:56Z");
    UUID jobId = UUID.fromString("0196b171-3f80-7000-8000-000000000001");

    JobLogEntity first = logEntry(UUID.fromString("0196b171-3f80-7000-8000-000000000101"), jobId, timestamp);
    JobLogEntity second = logEntry(UUID.fromString("0196b171-3f80-7000-8000-000000000202"), jobId, timestamp);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  private JobLogEntity logEntry(UUID id, UUID jobId, Instant timestamp) {
    JobLogEntity log = new JobLogEntity();
    log.setId(id);
    log.setJobId(jobId);
    log.setTs(timestamp);
    log.setLevel(JobLogEntity.LogLevel.INFO);
    log.setMessage("Job started");
    return log;
  }
}
