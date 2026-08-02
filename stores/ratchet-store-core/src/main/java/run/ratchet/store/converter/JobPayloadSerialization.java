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
package run.ratchet.store.converter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import run.ratchet.store.entity.JobPayload;

/**
 * Store-neutral payload serialization and per-thread staging for validated payloads.
 *
 * <p>This class deliberately has no persistence-API dependencies so non-JPA stores can share the
 * exact serialization accepted by creation-time validation.
 */
public final class JobPayloadSerialization {

  private static final ThreadLocal<Deque<Map<JobPayload, String>>> PREPARED_SERIALIZATIONS =
      new ThreadLocal<>();

  private JobPayloadSerialization() {}

  /** Starts a synchronous submission scope on the current thread. Scopes may be nested. */
  public static void beginPreparationScope() {
    Deque<Map<JobPayload, String>> scopes = PREPARED_SERIALIZATIONS.get();
    if (scopes == null) {
      scopes = new ArrayDeque<>();
      PREPARED_SERIALIZATIONS.set(scopes);
    }
    scopes.push(new IdentityHashMap<>());
  }

  /** Ends the current synchronous submission scope and releases every staged payload in it. */
  public static void endPreparationScope() {
    Deque<Map<JobPayload, String>> scopes = PREPARED_SERIALIZATIONS.get();
    if (scopes == null || scopes.isEmpty()) {
      return;
    }
    scopes.pop();
    if (scopes.isEmpty()) {
      PREPARED_SERIALIZATIONS.remove();
    }
  }

  /** Stages validated JSON for the synchronous persistence call on the current thread. */
  public static void prepareForPersistence(JobPayload payload, String serialized) {
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(serialized, "serialized");
    Deque<Map<JobPayload, String>> scopes = PREPARED_SERIALIZATIONS.get();
    if (scopes == null || scopes.isEmpty()) {
      return;
    }
    scopes.peek().put(payload, serialized);
  }

  /** Clears a creation-time serialization staged in the current submission scope. */
  public static void discardPreparedSerialization(JobPayload payload) {
    if (payload == null) {
      return;
    }
    Deque<Map<JobPayload, String>> scopes = PREPARED_SERIALIZATIONS.get();
    if (scopes != null && !scopes.isEmpty()) {
      scopes.peek().remove(payload);
    }
  }

  /** Clears every creation-time serialization staged on the current thread. */
  public static void discardAllPreparedSerializations() {
    PREPARED_SERIALIZATIONS.remove();
  }

  /** Serializes a payload, consuming validated JSON staged for the current submission when set. */
  public static String serialize(JobPayload payload) {
    if (payload == null) {
      return null;
    }
    try {
      String prepared = preparedSerialization(payload);
      if (prepared != null) {
        return prepared;
      }
      return PayloadSerializerHolder.get().serialize(payload);
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException("JobPayload serialization error", exception);
    }
  }

  /** Deserializes a stored JSON payload. */
  public static JobPayload deserialize(String stored) {
    if (stored == null || stored.isEmpty()) {
      return null;
    }
    try {
      return PayloadSerializerHolder.get().deserialize(stored, JobPayload.class);
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException("JobPayload deserialization error", exception);
    }
  }

  private static String preparedSerialization(JobPayload payload) {
    Deque<Map<JobPayload, String>> scopes = PREPARED_SERIALIZATIONS.get();
    if (scopes == null) {
      return null;
    }
    for (Map<JobPayload, String> prepared : scopes) {
      String serialized = prepared.get(payload);
      if (serialized != null) {
        return serialized;
      }
    }
    return null;
  }
}
