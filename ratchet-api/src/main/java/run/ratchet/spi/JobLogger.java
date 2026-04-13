package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Per-job logging facade. Implementations must be thread-safe. */
@Incubating
public interface JobLogger {

  /** Logs at INFO level. */
  void info(String message);

  /** Logs at DEBUG level. */
  void debug(String message);

  /** Logs at WARN level. */
  void warn(String message);

  /** Logs at ERROR level. */
  void error(String message);

  /** Logs at TRACE level. */
  void trace(String message);

  /** Logs at INFO level with SLF4J-style {@code {}} placeholder substitution. */
  default void info(String format, Object... args) {
    info(JobLoggerFormat.format(format, args));
  }

  /** Logs at DEBUG level with SLF4J-style {@code {}} placeholder substitution. */
  default void debug(String format, Object... args) {
    debug(JobLoggerFormat.format(format, args));
  }

  /** Logs at WARN level with SLF4J-style {@code {}} placeholder substitution. */
  default void warn(String format, Object... args) {
    warn(JobLoggerFormat.format(format, args));
  }

  /** Logs at ERROR level with SLF4J-style {@code {}} placeholder substitution. */
  default void error(String format, Object... args) {
    error(JobLoggerFormat.format(format, args));
  }

  /** Logs at TRACE level with SLF4J-style {@code {}} placeholder substitution. */
  default void trace(String format, Object... args) {
    trace(JobLoggerFormat.format(format, args));
  }
}
