package run.ratchet.loadtest.workload;

import java.util.Locale;

public enum WorkloadType {
  NOOP,
  SLEEP,
  PROBABILISTIC_FAILURE,
  MIXED;

  public static WorkloadType parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return NOOP;
    }
    String normalized = raw.trim().replace('-', '_').toUpperCase(Locale.ROOT);
    if ("PROBABILISTICFAILURE".equals(normalized)) {
      return PROBABILISTIC_FAILURE;
    }
    return WorkloadType.valueOf(normalized);
  }
}
