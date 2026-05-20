package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobExecutionType;

class MysqlJobRowMapperTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000403");

  @Test
  void hydrateWrapsInvalidEnumWithColumnContext() {
    Object[] row = liveRow();
    row[1] = "UNKNOWN_TYPE";

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> new MysqlJobRowMapper().hydrateJobEntity(row));

    assertTrue(thrown.getMessage().contains(JOB_ID.toString()));
    assertTrue(thrown.getMessage().contains("job_type"));
    assertTrue(thrown.getMessage().contains("UNKNOWN_TYPE"));
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
  }

  @Test
  void toInstantInterpretsLocalDateTimeAsUtc() {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));

      assertEquals(
          Instant.parse("2026-05-12T12:00:00Z"),
          MysqlJobRowMapper.toInstant(LocalDateTime.parse("2026-05-12T12:00:00")));
    } finally {
      TimeZone.setDefault(original);
    }
  }

  private static Object[] liveRow() {
    Object[] row = new Object[MysqlJobRowMapper.HYDRATION_COL_COUNT];
    Instant now = Instant.parse("2026-05-12T14:30:00Z");
    row[0] = JOB_ID.toString();
    row[1] = JobExecutionType.SINGLE.name();
    row[2] = JobPriority.NORMAL.ordinal();
    row[3] = 3;
    row[4] = BackoffPolicy.NONE.name();
    row[5] = 0;
    row[6] = 60;
    row[11] = "example.Job"; // target_class
    row[12] = "run"; // method_name
    row[20] = now; // created_at
    row[MysqlJobRowMapper.IDX_Q_STATUS] = "PENDING";
    row[MysqlJobRowMapper.IDX_Q_STATUS + 1] = now; // q.scheduled_time
    row[MysqlJobRowMapper.IDX_Q_STATUS + 2] = 0; // q.attempts
    row[MysqlJobRowMapper.IDX_Q_STATUS + 7] = 0; // q.version
    row[MysqlJobRowMapper.IDX_Q_STATUS + 8] = now; // q.updated_at
    return row;
  }
}
