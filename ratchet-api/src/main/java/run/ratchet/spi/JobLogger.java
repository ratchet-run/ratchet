package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Logger abstraction for job execution. Implementations may write to database log tables, SLF4J, or
 * other logging backends.
 */
@Incubating
public interface JobLogger {

  void info(String message);

  void debug(String message);

  void warn(String message);

  void error(String message);

  void trace(String message);
}
