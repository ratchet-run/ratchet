package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Per-job logging facade. Implementations must be thread-safe.
 *
 * <p>The default level checks return {@code true} so simple implementations log everything.
 * Production implementations should override them to avoid unnecessary message formatting.
 */
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

  /**
   * Returns whether informational messages should be formatted and logged.
   *
   * @return {@code true} when info logging is enabled
   */
  default boolean isInfoEnabled() {
    return true;
  }

  /**
   * Returns whether debug messages should be formatted and logged.
   *
   * @return {@code true} when debug logging is enabled
   */
  default boolean isDebugEnabled() {
    return true;
  }

  /**
   * Returns whether warning messages should be formatted and logged.
   *
   * @return {@code true} when warn logging is enabled
   */
  default boolean isWarnEnabled() {
    return true;
  }

  /**
   * Returns whether error messages should be formatted and logged.
   *
   * @return {@code true} when error logging is enabled
   */
  default boolean isErrorEnabled() {
    return true;
  }

  /**
   * Returns whether trace messages should be formatted and logged.
   *
   * @return {@code true} when trace logging is enabled
   */
  default boolean isTraceEnabled() {
    return true;
  }

  /**
   * Formats and logs an informational message using SLF4J-style {@code {}} placeholders.
   *
   * @param format message template; never {@code null}
   * @param args placeholder values
   */
  default void info(String format, Object... args) {
    if (isInfoEnabled()) {
      info(JobLoggerFormat.format(format, args));
    }
  }

  /**
   * Formats and logs a debug message using SLF4J-style {@code {}} placeholders.
   *
   * @param format message template; never {@code null}
   * @param args placeholder values
   */
  default void debug(String format, Object... args) {
    if (isDebugEnabled()) {
      debug(JobLoggerFormat.format(format, args));
    }
  }

  /**
   * Formats and logs a warning message using SLF4J-style {@code {}} placeholders.
   *
   * @param format message template; never {@code null}
   * @param args placeholder values
   */
  default void warn(String format, Object... args) {
    if (isWarnEnabled()) {
      warn(JobLoggerFormat.format(format, args));
    }
  }

  /**
   * Formats and logs an error message using SLF4J-style {@code {}} placeholders.
   *
   * @param format message template; never {@code null}
   * @param args placeholder values
   */
  default void error(String format, Object... args) {
    if (isErrorEnabled()) {
      error(JobLoggerFormat.format(format, args));
    }
  }

  /**
   * Formats and logs a trace message using SLF4J-style {@code {}} placeholders.
   *
   * @param format message template; never {@code null}
   * @param args placeholder values
   */
  default void trace(String format, Object... args) {
    if (isTraceEnabled()) {
      trace(JobLoggerFormat.format(format, args));
    }
  }
}
