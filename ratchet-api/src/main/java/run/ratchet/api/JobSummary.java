package run.ratchet.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Read-only projection of a job suitable for list views and search results.
 *
 * <p>All fields are derived from persisted state; none are computed at query time. For full detail
 * including execution history and parameters, use {@link JobDetail}.
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
    tags = tags == null ? null : List.copyOf(tags);
  }
}
