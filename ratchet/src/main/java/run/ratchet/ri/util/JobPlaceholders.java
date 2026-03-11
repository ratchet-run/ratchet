package run.ratchet.ri.util;

/**
 * Provides placeholder methods for job payloads that do not require actual execution.
 *
 * <p>This utility class contains static no-operation methods used as placeholders for
 * coordination-only jobs such as batch parent jobs. These jobs exist solely to track the lifecycle
 * of child jobs and do not perform any actual work themselves.
 *
 * <p>The methods in this class are intentionally empty and designed to be referenced via method
 * references in job payloads:
 *
 * <pre>{@code
 * JobPayload payload = JobPayloadFactory.fromLambda(JobPlaceholders::noop);
 * }</pre>
 */
public final class JobPlaceholders {

  /** Private constructor to prevent instantiation of this utility class. */
  private JobPlaceholders() {
    throw new UnsupportedOperationException("Utility class - do not instantiate");
  }

  /**
   * No-operation method for placeholder jobs.
   *
   * <p>This method performs no action and returns immediately. It is used as a payload for jobs
   * that exist only for coordination purposes, such as batch parent jobs that track the progress of
   * their children but do not execute any work themselves.
   */
  public static void noop() {
    // Intentionally empty - placeholder for coordination-only jobs
  }
}
