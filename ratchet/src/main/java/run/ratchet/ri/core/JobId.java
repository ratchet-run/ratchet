package run.ratchet.ri.core;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation that associates components with a specific job execution context. This annotation
 * enables dynamic selection of job-specific beans, particularly logger instances, allowing each job
 * to maintain its own isolated logging context.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Programmatic lookup of job-specific logger
 * JobLogger logger = loggerFactory.create(jobId);
 * }</pre>
 *
 * <p>This pattern enables:
 *
 * <ul>
 *   <li>Job-specific bean instances without manual factory management
 *   <li>Automatic cleanup when job context is destroyed
 *   <li>Type-safe association between jobs and their dependencies
 *   <li>Parallel execution with isolated contexts per job
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface JobId {

  /**
   * The unique identifier of the job execution context.
   *
   * @return the job ID value
   */
  long value();
}
