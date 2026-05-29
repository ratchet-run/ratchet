package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobPayload;

/**
 * Immutable value type representing a row in {@code scheduler_recurring_job}. Returned by {@link
 * RecurringJobStore} reads and consumed by writes.
 *
 * <p>Lives in {@code run.ratchet.store.spi} rather than {@code run.ratchet.api} because {@link
 * JobPayload} is a store-core type. Other SPI sub-interfaces follow the same convention.
 *
 * <p>{@code onSuccessPayload}, {@code onFailurePayload}, {@code businessKey}, {@code resourceName},
 * {@code executionTarget}, and {@code callerPrincipal} are nullable. {@code pausedAt} is nullable
 * and must be {@code null} iff {@code paused} is {@code false}.
 *
 * @param id recurring-master primary key; never {@code null}
 * @param cronExpr cron expression that drives {@code nextFire}; never {@code null} or blank
 * @param zoneId IANA zone id used to evaluate {@code cronExpr}; never {@code null} or blank
 * @param nextFire next scheduled fire instant for this master; never {@code null}
 * @param paused {@code true} when the master is currently paused (not eligible for claim)
 * @param pausedAt timestamp at which the master was paused; MUST be {@code null} iff {@code paused
 *     == false}, MUST be non-{@code null} iff {@code paused == true}
 * @param priority job priority assigned to fired child jobs
 * @param maxRetries maximum retry attempts assigned to fired child jobs
 * @param backoffPolicy retry backoff policy assigned to fired child jobs; never {@code null}
 * @param backoffParamMs backoff policy parameter (interpretation depends on {@code backoffPolicy})
 * @param timeoutSec execution timeout assigned to fired child jobs, in seconds
 * @param payload payload template for fired child jobs; never {@code null}
 * @param onSuccessPayload optional success-callback payload, or {@code null} when none configured
 * @param onFailurePayload optional failure-callback payload, or {@code null} when none configured
 * @param businessKey business key carried into fired child jobs, or {@code null} when none
 *     configured
 * @param resourceName resource permit name required by fired child jobs, or {@code null} when no
 *     resource gate applies
 * @param executionTarget execution-target label copied to fired child jobs, or {@code null} to
 *     inherit the deployment default
 * @param createdAt instant the master was registered; never {@code null}
 * @param callerPrincipal caller principal captured at registration, or {@code null} when no
 *     security context was present
 */
@Incubating
public record RecurringJobDefinition(
    UUID id,
    String cronExpr,
    String zoneId,
    Instant nextFire,
    boolean paused,
    Instant pausedAt,
    int priority,
    int maxRetries,
    BackoffPolicy backoffPolicy,
    int backoffParamMs,
    int timeoutSec,
    JobPayload payload,
    JobPayload onSuccessPayload,
    JobPayload onFailurePayload,
    String businessKey,
    String resourceName,
    String executionTarget,
    Instant createdAt,
    String callerPrincipal) {}
