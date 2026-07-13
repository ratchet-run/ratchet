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
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;

/**
 * Immutable correlation projection of an archived recurring definition. Payload templates are
 * intentionally omitted: this type resolves durable recurring lineage and is not a replay surface.
 *
 * @param id archived recurring-master primary key; never {@code null}
 * @param businessKey business key that identified the recurring definition, or {@code null} when
 *     none was configured
 * @param cronExpr cron expression that drove the recurring definition; never {@code null} or blank
 * @param zoneId IANA zone id used to evaluate {@code cronExpr}; never {@code null} or blank
 * @param executionTarget execution-target label used by fired child jobs, or {@code null} when the
 *     deployment default applied
 * @param callerPrincipal caller principal captured at registration, or {@code null} when no
 *     security context was present
 * @param createdAt instant the recurring definition was registered; never {@code null}
 * @param archivedAt instant the recurring definition was archived; never {@code null}
 * @param archiveReason reason the recurring definition was archived; never {@code null}
 */
@Incubating
public record ArchivedRecurringJob(
    UUID id,
    String businessKey,
    String cronExpr,
    String zoneId,
    String executionTarget,
    String callerPrincipal,
    Instant createdAt,
    Instant archivedAt,
    ArchiveReason archiveReason) {}
