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
package run.ratchet.store.dto;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import run.ratchet.api.Nullable;

/**
 * One outbox read result, retaining its deletable id even when its snapshot cannot be decoded.
 * Exactly one of {@code event} and {@code decodingFailure} is non-null.
 *
 * @param eventId stored event id; never null
 * @param event decoded snapshot, or null on a decoding failure
 * @param decodingFailure reason the snapshot is malformed, or null on success
 */
public record OutboxEventRead(
    UUID eventId, @Nullable OutboxEvent event, @Nullable RuntimeException decodingFailure) {
  public OutboxEventRead {
    Objects.requireNonNull(eventId, "eventId");
    if ((event == null) == (decodingFailure == null)) {
      throw new IllegalArgumentException("Exactly one of event and decodingFailure is required");
    }
    if (event != null && !eventId.equals(event.eventId())) {
      throw new IllegalArgumentException("eventId must match the snapshot");
    }
  }

  /** Wraps a successfully decoded snapshot. */
  public static OutboxEventRead decoded(OutboxEvent event) {
    return new OutboxEventRead(event.eventId(), event, null);
  }

  /**
   * Decodes an already fetched row, retaining invalid field values as a per-row failure. Read and
   * validate the event id before calling this method: an unreadable id fails the batch because the
   * row cannot safely be deleted through the UUID-based store API.
   *
   * <p>Only field-conversion and snapshot-validation failures are captured. Database queries and
   * cursor iteration must happen outside the decoder; store failures (including failures reading
   * SQL LOBs) propagate.
   *
   * @param eventId readable stored event id; never null
   * @param decoder conversion of fetched fields into a snapshot; never null
   * @return decoded snapshot or its conversion failure, with the supplied id
   */
  public static OutboxEventRead decode(UUID eventId, Supplier<OutboxEvent> decoder) {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(decoder, "decoder");
    try {
      return new OutboxEventRead(eventId, decoder.get(), null);
    } catch (IllegalArgumentException | NullPointerException | ClassCastException failure) {
      return new OutboxEventRead(eventId, null, failure);
    }
  }
}
