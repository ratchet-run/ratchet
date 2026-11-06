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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.dto.OutboxEvent;

/**
 * Optional durable terminal-event outbox. Rows are inserted atomically by {@link
 * JobTerminalStore#commitCompletion}; listeners can deduplicate redelivery by event id.
 * Backing-store failures propagate; implementations must not convert failures to an empty result or
 * a zero delete count. Callers probe via {@code jobStore.capability(EventOutboxStore.class)}.
 */
@Incubating
public interface EventOutboxStore {
  /**
   * Reads events created strictly before the cutoff, ordered by created_at then event_id.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   *
   * @param cutoff exclusive creation-time bound; never null
   * @param limit maximum rows to return; at least one
   * @return at most limit immutable event snapshots
   * @throws IllegalArgumentException when limit is less than one
   * @throws NullPointerException when cutoff is null
   * @throws RuntimeException if the backing store cannot complete the read
   */
  List<OutboxEvent> findEventsCreatedBefore(Instant cutoff, int limit);

  /**
   * Deletes the supplied event ids, ignoring missing ids. An empty collection returns zero without
   * a store call. Callers must pass at most 500 ids, keeping every supported database within its
   * IN-list and parameter limits.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   *
   * @param eventIds ids to delete; neither the collection nor its entries may be null
   * @return number of deleted rows
   * @throws IllegalArgumentException when more than 500 ids are supplied
   * @throws NullPointerException when eventIds or any entry is null
   * @throws RuntimeException if the backing store cannot complete the deletion
   */
  int deleteEvents(Collection<UUID> eventIds);
}
