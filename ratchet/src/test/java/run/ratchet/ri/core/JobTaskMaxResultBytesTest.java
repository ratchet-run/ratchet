package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JobTask#maxResultBytes()} — the system-property-driven cap on serialized
 * job result size used by {@code handleSuccess} truncation logic.
 */
class JobTaskMaxResultBytesTest {

  @AfterEach
  void clearProperty() {
    System.clearProperty(JobTask.RESULT_MAX_BYTES_PROPERTY);
  }

  @Test
  void defaultsTo64KbWhenUnset() {
    System.clearProperty(JobTask.RESULT_MAX_BYTES_PROPERTY);
    assertEquals(65536L, JobTask.maxResultBytes());
  }

  @Test
  void honorsExplicitProperty() {
    System.setProperty(JobTask.RESULT_MAX_BYTES_PROPERTY, "1024");
    assertEquals(1024L, JobTask.maxResultBytes());
  }

  @Test
  void zeroDisablesCap() {
    System.setProperty(JobTask.RESULT_MAX_BYTES_PROPERTY, "0");
    assertEquals(0L, JobTask.maxResultBytes());
  }

  @Test
  void clampsNegativeToZero() {
    System.setProperty(JobTask.RESULT_MAX_BYTES_PROPERTY, "-100");
    assertEquals(0L, JobTask.maxResultBytes());
  }

  @Test
  void fallsBackToDefaultOnInvalidProperty() {
    System.setProperty(JobTask.RESULT_MAX_BYTES_PROPERTY, "not-a-number");
    assertEquals(65536L, JobTask.maxResultBytes());
  }
}
