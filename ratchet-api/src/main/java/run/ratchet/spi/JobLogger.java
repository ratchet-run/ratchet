package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Defines a logging interface specifically for job-related operations, allowing log messages to be
 * categorized into various levels of severity. This interface is designed to be simple and flexible
 * for integration with different logging backends.
 *
 * <p>The {@code JobLogger} interface is marked as {@link Incubating}, indicating that it is
 * experimental and subject to potential changes in future releases. Implementations of this
 * interface should adhere to its contract while remaining thread-safe.
 *
 * <p>Log levels supported by this interface: - INFO: General informational messages that highlight
 * job progress or milestones. - DEBUG: Detailed diagnostic messages useful during development or
 * debugging. - WARN: Messages that indicate potentially problematic situations. - ERROR: Messages
 * that signify significant failures or errors during job execution. - TRACE: Fine-grained messages
 * for understanding execution paths or low-level operations.
 *
 * <p>Usage of this interface assumes that implementations will provide appropriate behavior to
 * handle the logging in the context of their environment (e.g., output to consoles, files, or
 * external systems).
 */
@Incubating
public interface JobLogger {

  /**
   * Logs an informational message indicating job progress or milestones. This log level is
   * primarily used for general-purpose informational messages that signify normal application
   * behavior.
   *
   * @param message the informational message to be logged; cannot be null.
   */
  void info(String message);

  /**
   * Logs a detailed diagnostic message typically useful during development or debugging. This log
   * level provides fine-grained information helpful for troubleshooting issues or understanding
   * application behavior in detail.
   *
   * @param message the diagnostic message to be logged; must not be null.
   */
  void debug(String message);

  /**
   * Logs a warning message that indicates a potentially problematic situation. This log level is
   * used to highlight occurrences that may require attention but do not necessarily signify errors
   * or failures.
   *
   * @param message the warning message to be logged; must not be null.
   */
  void warn(String message);

  /**
   * Logs an error message indicating a significant failure or issue encountered during job
   * execution. This log level is used to report errors that prevent normal operation or require
   * immediate attention.
   *
   * @param message the error message to be logged; must not be null.
   */
  void error(String message);

  /**
   * Logs a fine-grained message useful for understanding execution paths or low-level operations.
   * This log level is typically used for detailed tracing during development or troubleshooting,
   * capturing verbose information about the application's behavior.
   *
   * @param message the trace message to be logged; must not be null.
   */
  void trace(String message);
}
