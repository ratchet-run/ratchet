package run.ratchet.loadtest.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LoadTestWorkloadExecutorTest {

  @Test
  void executeRejectsMalformedStringArgumentsWithFieldName() {
    LoadTestWorkloadExecutor executor = new LoadTestWorkloadExecutor();

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executor.execute("run-1", "noop", "not-a-number", "0", "0", "0.0", "0", "0.0", ""));

    assertEquals("sequence must be a valid integer: not-a-number", thrown.getMessage());
    assertInstanceOf(NumberFormatException.class, thrown.getCause());
  }

  @Test
  void executeRejectsMalformedDecimalArgumentsWithFieldName() {
    LoadTestWorkloadExecutor executor = new LoadTestWorkloadExecutor();

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> executor.execute("run-1", "noop", "1", "0", "0", "bad", "0", "0.0", ""));

    assertEquals("sleepSpikeRate must be a valid decimal: bad", thrown.getMessage());
    assertInstanceOf(NumberFormatException.class, thrown.getCause());
  }
}
