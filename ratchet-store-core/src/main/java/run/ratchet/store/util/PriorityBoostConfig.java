package run.ratchet.store.util;

/**
 * Shared helper for reading the priority-boost interval env var used by store implementations.
 *
 * <p>Reads {@code RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES} first, falling back to {@code
 * SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES} for backward compatibility. This precedence matches
 * {@code RatchetConfiguration.getPriorityBoostIntervalMinutes()} in the reference implementation,
 * so operators can set the env var once and have both the RI and store layers agree on the value.
 *
 * <p>An invalid (non-numeric, negative, or unparseable) value falls back to the default of {@code
 * 15}. Setting the value to {@code 0} disables priority boosting.
 */
public final class PriorityBoostConfig {

  private static final int DEFAULT_MINUTES = 15;

  private PriorityBoostConfig() {}

  public static int getPriorityBoostIntervalMinutes() {
    String raw = System.getenv("RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES");
    if (raw == null || raw.isBlank()) {
      raw = System.getenv("SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES");
    }
    if (raw == null || raw.isBlank()) {
      return DEFAULT_MINUTES;
    }
    try {
      return Math.max(0, Integer.parseInt(raw.trim()));
    } catch (NumberFormatException e) {
      return DEFAULT_MINUTES;
    }
  }
}
