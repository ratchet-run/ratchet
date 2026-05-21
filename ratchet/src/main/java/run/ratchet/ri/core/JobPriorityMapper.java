package run.ratchet.ri.core;

import run.ratchet.api.JobPriority;

/**
 * Maps stored priority ordinals back to {@link JobPriority}. Recurring masters carry the ordinal as
 * an int so the SPI doesn't drag the enum into store-core; reading code has to clamp because the
 * persisted value may predate enum additions or, in pathological cases, be corrupt.
 */
final class JobPriorityMapper {

  private static final int MAX_ORDINAL = JobPriority.values().length - 1;

  private JobPriorityMapper() {}

  static JobPriority fromOrdinal(int priority) {
    int clamped = Math.max(0, Math.min(priority, MAX_ORDINAL));
    return JobPriority.values()[clamped];
  }
}
