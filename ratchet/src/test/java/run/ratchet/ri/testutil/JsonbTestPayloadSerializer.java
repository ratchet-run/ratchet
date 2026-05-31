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
package run.ratchet.ri.testutil;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import run.ratchet.spi.PayloadSerializer;

/**
 * JSON-B-backed {@link PayloadSerializer} implementation for unit tests. Allows tests to obtain a
 * realistic serializer without depending on the CDI-managed {@code JsonbPayloadSerializer} bean.
 */
public final class JsonbTestPayloadSerializer implements PayloadSerializer {

  private final Jsonb jsonb = JsonbBuilder.create();

  @Override
  public String serialize(Object payload) {
    if (payload == null) {
      return null;
    }
    try {
      return jsonb.toJson(payload);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B serialization error for " + payload.getClass().getName(), e);
    }
  }

  @Override
  public <T> T deserialize(String json, Class<T> type) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return jsonb.fromJson(json, type);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B deserialization error for " + (type == null ? "null" : type.getName()), e);
    }
  }
}
