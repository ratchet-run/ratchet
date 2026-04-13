package run.ratchet.api;

/**
 * Functional interface for submitting a configured job for execution.
 *
 * <p>This decouples {@link JobBuilder} from the concrete scheduler service. The reference
 * implementation passes {@code this::persistJob} as the submitter.
 */
@FunctionalInterface
public interface JobSubmitter {

  /** Submits the fully-configured job builder for persistence and scheduling. */
  JobHandle submit(JobBuilder builder);
}
