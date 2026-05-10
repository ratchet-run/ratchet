package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobExecutionType;

class ArchiveRowMapperTest {

  @Test
  void mapRejectsNullEnumColumn() {
    Object[] row = validRow();
    row[2] = null;

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> ArchiveRowMapper.map(row, this::instant));

    assertTrue(ex.getMessage().contains("final_status"));
    assertTrue(ex.getMessage().contains("must not be null"));
  }

  @Test
  void mapRejectsInvalidPriorityOrdinal() {
    Object[] row = validRow();
    row[4] = 99;

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> ArchiveRowMapper.map(row, this::instant));

    assertTrue(ex.getMessage().contains("priority"));
    assertTrue(ex.getMessage().contains("99"));
    assertTrue(ex.getMessage().contains("0..4"));
  }

  @Test
  void mapAcceptsValidProjection() {
    Object[] row = validRow();

    ArchivedJobEntity archive = ArchiveRowMapper.map(row, this::instant);

    assertEquals(JobStatus.SUCCEEDED, archive.getFinalStatus());
    assertEquals(JobExecutionType.SINGLE, archive.getJobType());
    assertEquals(JobPriority.NORMAL, archive.getPriority());
    assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000001"), archive.getId());
    assertEquals(
        UUID.fromString("00000000-0000-0000-0000-000000000002"), archive.getOriginalJobId());
    assertEquals(1, archive.getTotalAttempts());
    assertEquals(3, archive.getMaxRetries());
    assertEquals(BackoffPolicy.NONE, archive.getBackoffPolicy());
    assertEquals(0, archive.getBackoffParamMs());
    assertEquals(60, archive.getTimeoutSec());
    assertEquals("com.example.Job", archive.getTargetClass());
    assertEquals("run", archive.getMethodName());
    assertEquals("business-key", archive.getBusinessKey());
    assertEquals(Instant.parse("2026-05-07T12:00:00Z"), archive.getOriginalScheduledTime());
    assertEquals(Instant.parse("2026-05-07T12:00:01Z"), archive.getOriginalCreatedAt());
    assertEquals(Instant.parse("2026-05-07T12:00:02Z"), archive.getFirstExecutionTime());
    assertEquals(Instant.parse("2026-05-07T12:00:03Z"), archive.getCompletionTime());
    assertEquals(100L, archive.getTotalExecutionTimeMs());
    assertEquals(200L, archive.getQueueWaitMs());
    assertEquals(Instant.parse("2026-05-07T12:00:04Z"), archive.getArchivedAt());
    assertEquals("archiver", archive.getArchivedBy());
    assertEquals("retention", archive.getArchiveReason());
    assertEquals("result", archive.getJobResult());
    assertEquals("java.lang.String", archive.getResultType());
    assertEquals("payload", archive.getPayloadSummary());
    assertEquals("tag", archive.getTags());
  }

  @Test
  void mapAcceptsUuidBytesFromNativeMysqlProjection() {
    Object[] row = validRow();
    UUID id = UUID.fromString("00000000-0000-0000-0000-000000000011");
    UUID originalJobId = UUID.fromString("00000000-0000-0000-0000-000000000012");
    row[0] = uuidBytes(id);
    row[1] = uuidBytes(originalJobId);

    ArchivedJobEntity archive = ArchiveRowMapper.map(row, this::instant);

    assertEquals(id, archive.getId());
    assertEquals(originalJobId, archive.getOriginalJobId());
  }

  private Object[] validRow() {
    Object[] row = new Object[ArchiveRowMapper.COLUMN_COUNT];
    int column = 0;
    row[column++] = UUID.fromString("00000000-0000-0000-0000-000000000001");
    row[column++] = UUID.fromString("00000000-0000-0000-0000-000000000002");
    row[column++] = JobStatus.SUCCEEDED.name();
    row[column++] = JobExecutionType.SINGLE.name();
    row[column++] = JobPriority.NORMAL.ordinal();
    row[column++] = 1;
    row[column++] = 3;
    row[column++] = BackoffPolicy.NONE.name();
    row[column++] = 0;
    row[column++] = 60;
    row[column++] = "com.example.Job";
    row[column++] = "run";
    row[column++] = "business-key";
    row[column++] = null;
    row[column++] = null;
    row[column++] = Instant.parse("2026-05-07T12:00:00Z");
    row[column++] = Instant.parse("2026-05-07T12:00:01Z");
    row[column++] = Instant.parse("2026-05-07T12:00:02Z");
    row[column++] = Instant.parse("2026-05-07T12:00:03Z");
    row[column++] = 100L;
    row[column++] = 200L;
    row[column++] = Instant.parse("2026-05-07T12:00:04Z");
    row[column++] = "archiver";
    row[column++] = "retention";
    row[column++] = "result";
    row[column++] = "java.lang.String";
    row[column++] = null;
    row[column++] = "payload";
    row[column++] = null;
    row[column++] = null;
    row[column] = "tag";
    return row;
  }

  private Instant instant(Object value) {
    return (Instant) value;
  }

  private static byte[] uuidBytes(UUID uuid) {
    return ByteBuffer.allocate(16)
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits())
        .array();
  }
}
