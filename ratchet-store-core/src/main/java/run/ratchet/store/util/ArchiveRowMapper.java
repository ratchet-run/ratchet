package run.ratchet.store.util;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

/** Maps explicit archive native-query projections into {@link ArchivedJobEntity} instances. */
public final class ArchiveRowMapper {

  public static final int COLUMN_COUNT = 31;

  private ArchiveRowMapper() {}

  public static ArchivedJobEntity map(Object[] row, Function<Object, Instant> instantMapper) {
    assertColumnCount(row);
    ArchivedJobEntity archive = new ArchivedJobEntity();
    int column = 0;
    archive.setId(uuidOrNull(row[column++]));
    archive.setOriginalJobId(uuidOrNull(row[column++]));
    archive.setFinalStatus(JobStatus.valueOf(stringOrNull(row[column++])));
    archive.setJobType(JobExecutionType.valueOf(stringOrNull(row[column++])));
    archive.setPriority(JobPriority.values()[((Number) row[column++]).intValue()]);
    archive.setTotalAttempts(((Number) row[column++]).intValue());
    archive.setMaxRetries(((Number) row[column++]).intValue());
    archive.setBackoffPolicy(BackoffPolicy.valueOf(stringOrNull(row[column++])));
    archive.setBackoffParamMs(((Number) row[column++]).intValue());
    archive.setTimeoutSec(((Number) row[column++]).intValue());
    archive.setTargetClass(stringOrNull(row[column++]));
    archive.setMethodName(stringOrNull(row[column++]));
    archive.setBusinessKey(stringOrNull(row[column++]));
    archive.setCronExpr(stringOrNull(row[column++]));
    archive.setZoneId(stringOrNull(row[column++]));
    archive.setOriginalScheduledTime(instantMapper.apply(row[column++]));
    archive.setOriginalCreatedAt(instantMapper.apply(row[column++]));
    archive.setFirstExecutionTime(instantMapper.apply(row[column++]));
    archive.setCompletionTime(instantMapper.apply(row[column++]));
    archive.setTotalExecutionTimeMs(longOrNull(row[column++]));
    archive.setQueueWaitMs(longOrNull(row[column++]));
    archive.setArchivedAt(instantMapper.apply(row[column++]));
    archive.setArchivedBy(stringOrNull(row[column++]));
    archive.setArchiveReason(stringOrNull(row[column++]));
    archive.setJobResult(stringOrNull(row[column++]));
    archive.setResultType(stringOrNull(row[column++]));
    archive.setFinalError(stringOrNull(row[column++]));
    archive.setPayloadSummary(stringOrNull(row[column++]));
    archive.setDependedOn(uuidOrNull(row[column++]));
    archive.setSupersededBy(uuidOrNull(row[column++]));
    archive.setTags(stringOrNull(row[column]));
    return archive;
  }

  private static void assertColumnCount(Object[] row) {
    if (row == null || row.length != COLUMN_COUNT) {
      throw new IllegalStateException(
          "Expected archive row with "
              + COLUMN_COUNT
              + " columns but got "
              + (row == null ? "null" : row.length));
    }
  }

  private static Long longOrNull(Object value) {
    return value == null ? null : ((Number) value).longValue();
  }

  private static UUID uuidOrNull(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof UUID uuid) {
      return uuid;
    }
    if (value instanceof byte[] bytes) {
      return uuidFromBytes(bytes);
    }
    return UUID.fromString(value.toString());
  }

  private static UUID uuidFromBytes(byte[] bytes) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException("UUID byte array must be 16 bytes, got " + bytes.length);
    }
    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (bytes[i] & 0xff);
    }
    for (int i = 8; i < 16; i++) {
      lsb = (lsb << 8) | (bytes[i] & 0xff);
    }
    return new UUID(msb, lsb);
  }

  private static String stringOrNull(Object value) {
    return value == null ? null : value.toString();
  }
}
