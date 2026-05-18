package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Creates the {@link JobLogger} bound into {@code JobContext} for each execution.
 *
 * @since 0.1
 */
@Incubating
public interface JobLoggerFactory {

  /**
   * Creates the logger bound to one job execution.
   *
   * <p>The RI calls this once per execution attempt before binding {@code JobContext}. Returning
   * {@code null} violates the SPI contract. Implementations may throw a runtime exception when a
   * required logging backend is unavailable; callers treat that as an execution setup failure.
   *
   * @param context immutable execution logging context; never {@code null}
   * @return logger for this execution attempt; never {@code null}
   */
  JobLogger create(JobLoggerContext context);
}
