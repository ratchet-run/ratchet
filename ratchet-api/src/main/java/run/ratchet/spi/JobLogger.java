package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Per-job logging facade. Implementations must be thread-safe. */
@Incubating
public interface JobLogger {

  void info(String message);

  void debug(String message);

  void warn(String message);

  void error(String message);

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
