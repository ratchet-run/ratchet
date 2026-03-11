package run.ratchet.api;

/**
 * Functional interface for submitting a configured job for execution.
 *
 * <p>This decouples {@link JobBuilder} from the concrete scheduler service. The reference
 * implementation passes {@code this::persistJob} as the submitter.
 */
@FunctionalInterface
public interface JobSubmitter {

  /**
   * Submits the fully-configured job builder for persistence and scheduling.
   *
   * @param builder the job builder containing all configuration
   * @return a handle to the submitted job
   */
  JobHandle submit(JobBuilder builder);
}
