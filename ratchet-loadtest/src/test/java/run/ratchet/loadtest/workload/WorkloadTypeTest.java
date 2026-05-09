package run.ratchet.loadtest.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class WorkloadTypeTest {

  @Test
  void parseDefaultsBlankInputToNoop() {
    assertEquals(WorkloadType.NOOP, WorkloadType.parse(null));
    assertEquals(WorkloadType.NOOP, WorkloadType.parse(""));
    assertEquals(WorkloadType.NOOP, WorkloadType.parse("   "));
  }

  @Test
  void parseNormalizesCommonInputShapes() {
    assertEquals(WorkloadType.SLEEP, WorkloadType.parse(" sleep "));
    assertEquals(WorkloadType.PROBABILISTIC_FAILURE, WorkloadType.parse("probabilistic-failure"));
    assertEquals(WorkloadType.PROBABILISTIC_FAILURE, WorkloadType.parse("probabilisticFailure"));
  }

  @Test
  void parseRejectsUnknownWorkload() {
    assertThrows(IllegalArgumentException.class, () -> WorkloadType.parse("unknown"));
  }
}
