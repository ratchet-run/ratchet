package run.ratchet.api;

/**
 * Functional interface for submitting a configured job for execution.
 *
 * <p>This decouples {@link JobBuilder} from the concrete scheduler service. The reference
 * implementation implements {@code JobSubmitter} on its job-creation service.
 */
@FunctionalInterface
public interface JobSubmitter {

  /**
   * Submits the fully-configured job builder for persistence and scheduling.
   *
   * @param builder non-null builder to validate and persist
   * @return non-null handle for the persisted job, including its assigned UUIDv7 id
   * @throws NullPointerException if {@code builder} is null
   * @throws RuntimeException if validation, authorization, serialization, or persistence fails
   */
  JobHandle submit(JobBuilder builder);
}
