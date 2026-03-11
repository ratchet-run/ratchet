package run.ratchet.api;

/**
 * A lightweight handle to a submitted job providing access to its unique identifier.
 *
 * <p>JobHandle serves as a receipt for job submission, allowing clients to track and reference jobs
 * after they have been enqueued in the scheduler. This interface provides a minimal, immutable
 * reference that can be safely passed between components or stored for later use.
 *
 * <h2>Purpose:</h2>
 *
 * <ul>
 *   <li>Provides immediate confirmation of successful job submission
 *   <li>Enables job tracking and monitoring through the job ID
 *   <li>Allows correlation between job submission and execution
 *   <li>Facilitates job queries and status checks
 * </ul>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * // Submit a job and get its handle
 * JobHandle handle = schedulerService.enqueue(() -> processData())
 *     .withPriority(JobPriority.HIGH)
 *     .submit();
 *
 * // Use the handle to track the job
 * long jobId = handle.id();
 * log.info("Submitted job with ID: {}", jobId);
 *
 * // Later, use the ID to query job status
 * JobStatus status = schedulerService.getJobStatus(jobId);
 * }</pre>
 *
 * <h2>Implementation Note:</h2>
 *
 * <p>Implementations of this interface are typically lightweight, containing only the job ID. They
 * are designed to be thread-safe and can be freely shared between threads or components.
 *
 * @see JobBuilder#submit()
 * @see BatchBuilder#submit()
 * @see RecurringJobBuilder#submit()
 */
@FunctionalInterface
public interface JobHandle {

  /**
   * Returns the unique identifier of the submitted job.
   *
   * <p>This ID is globally unique within the scheduler and remains valid throughout the job's
   * lifecycle, including after completion. The ID can be used to:
   *
   * <ul>
   *   <li>Query job status and progress
   *   <li>Cancel or modify the job (if supported)
   *   <li>Retrieve job execution results
   *   <li>Correlate logs and monitoring data
   * </ul>
   *
   * @return the unique job identifier assigned by the scheduler
   */
  long id();
}
