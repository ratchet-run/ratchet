package run.ratchet.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Read-only projection of a job suitable for list views and search results.
 *
 * <p>All fields are derived from persisted state; none are computed at query time. For full detail
 * including execution history and parameters, use {@link JobDetail}.
 *
 * @param id job identifier
 * @param status current lifecycle status
 * @param type execution type
 * @param priority scheduling priority
 * @param businessKey caller-supplied business key, or null
 * @param idempotencyKey caller-supplied idempotency key, or null
 * @param targetClass scheduled target class name
 * @param methodName scheduled target method name
 * @param tags job tags; empty if the store projection did not include tags
 * @param resourceName resource permit name, or null
 * @param pickedBy worker node currently running the job, or null
 * @param createdAt persisted creation time
 * @param scheduledTime next scheduled execution time
 * @param updatedAt last persisted update time
 * @param callerPrincipal principal captured at creation, or null
 * @param lastError most recent error message, or null
 * @param attempts number of recorded attempts
 * @param maxRetries maximum configured retries
 * @param dependsOn parent dependency id, or null
 */
@Incubating
public record JobSummary(
    UUID id,
    JobStatus status,
    JobType type,
    JobPriority priority,
    String businessKey,
    String idempotencyKey,
    String targetClass,
    String methodName,
    List<String> tags,
    String resourceName,
    String pickedBy,
    Instant createdAt,
    Instant scheduledTime,
    Instant updatedAt,
    String callerPrincipal,
    String lastError,
    int attempts,
    int maxRetries,
    UUID dependsOn) {
  public JobSummary {
    tags = tags == null ? List.of() : List.copyOf(tags);
  }
}
