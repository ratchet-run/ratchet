package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Per-job logging facade. Implementations must be thread-safe. */
@Incubating
public interface JobLogger {

  /**
   * Logs an informational message for the current job.
   *
   * @param message already formatted message; never {@code null}
   */
  void info(String message);

  /**
   * Logs a debug message for the current job.
   *
   * @param message already formatted message; never {@code null}
   */
  void debug(String message);

  /**
   * Logs a warning message for the current job.
   *
   * @param message already formatted message; never {@code null}
   */
  void warn(String message);

  /**
   * Logs an error message for the current job.
   *
   * @param message already formatted message; never {@code null}
   */
  void error(String message);

  /**
   * Logs a trace message for the current job.
   *
   * @param message already formatted message; never {@code null}
   */
  void trace(String message);

  default void info(String format, Object... args) {
    info(JobLoggerFormat.format(format, args));
  }

  default void debug(String format, Object... args) {
    debug(JobLoggerFormat.format(format, args));
  }

  default void warn(String format, Object... args) {
    warn(JobLoggerFormat.format(format, args));
  }

  default void error(String format, Object... args) {
    error(JobLoggerFormat.format(format, args));
  }

  default void trace(String format, Object... args) {
    trace(JobLoggerFormat.format(format, args));
  }
}
