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
 * <p>{@code params}, {@code onSuccessPayload}, {@code onFailurePayload}, {@code businessKey},
 * {@code resourceName}, and {@code callerPrincipal} are nullable. {@code pausedAt} is nullable and
 * must be {@code null} iff {@code paused} is {@code false}.
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
    JobPayload params,
    JobPayload onSuccessPayload,
    JobPayload onFailurePayload,
    String businessKey,
    String resourceName,
    Instant createdAt,
    String callerPrincipal) {}
