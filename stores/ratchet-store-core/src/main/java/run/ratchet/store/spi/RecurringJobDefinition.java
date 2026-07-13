/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.spi;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.Incubating;
import run.ratchet.api.RecurringMisfirePolicy;
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
 * @param businessKey active-unique identity of the recurring master, or {@code null} when none
 *     configured; fired child jobs do not inherit it
 * @param resourceName resource permit name required by fired child jobs, or {@code null} when no
 *     resource gate applies
 * @param executionTarget execution-target label copied to fired child jobs, or {@code null} to
 *     inherit the deployment default
 * @param createdAt instant the master was registered; never {@code null}
 * @param callerPrincipal caller principal captured at registration, or {@code null} when no
 *     security context was present
 * @param encryptedPayload whether this master opted into payload encryption; fired child jobs
 *     inherit it, and the master's own {@code payload}/{@code on_success_payload}/{@code
 *     on_failure_payload} templates are encrypted at rest when it (or the global switch) is on
 * @param misfirePolicy persisted policy for handling a backlog of missed cron occurrences; never
 *     {@code null}
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
    String callerPrincipal,
    boolean encryptedPayload,
    RecurringMisfirePolicy misfirePolicy) {

  public RecurringJobDefinition {
    Objects.requireNonNull(misfirePolicy, "misfirePolicy must not be null");
  }
}
