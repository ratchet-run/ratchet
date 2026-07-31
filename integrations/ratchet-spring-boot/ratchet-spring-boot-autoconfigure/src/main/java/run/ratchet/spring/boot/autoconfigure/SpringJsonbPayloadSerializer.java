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
package run.ratchet.spring.boot.autoconfigure;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import run.ratchet.spi.PayloadSerializer;

/** JSON-B payload serializer with explicit borrowed-versus-owned {@link Jsonb} lifecycle. */
final class SpringJsonbPayloadSerializer implements PayloadSerializer, DisposableBean {

  private static final Log LOG = LogFactory.getLog(SpringJsonbPayloadSerializer.class);

  private final Jsonb jsonb;
  private final boolean ownsJsonb;
  private final AtomicBoolean closed = new AtomicBoolean();

  SpringJsonbPayloadSerializer(Jsonb jsonb, boolean ownsJsonb) {
    this.jsonb = Objects.requireNonNull(jsonb, "jsonb must not be null");
    this.ownsJsonb = ownsJsonb;
  }

  @Override
  public String serialize(Object payload) {
    if (payload == null) {
      return null;
    }
    try {
      return jsonb.toJson(payload);
    } catch (JsonbException exception) {
      throw new IllegalArgumentException(
          "JSON-B serialization error for " + payload.getClass().getName(), exception);
    }
  }

  @Override
  public <T> T deserialize(String json, Class<T> type) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return jsonb.fromJson(json, type);
    } catch (JsonbException exception) {
      throw new IllegalArgumentException(
          "JSON-B deserialization error for " + (type == null ? "null" : type.getName()),
          exception);
    }
  }

  @Override
  public void destroy() {
    if (!ownsJsonb || !closed.compareAndSet(false, true)) {
      return;
    }
    try {
      jsonb.close();
    } catch (Exception exception) {
      LOG.warn("Failed to close Ratchet-owned Jsonb instance during shutdown", exception);
    }
  }

  boolean ownsJsonb() {
    return ownsJsonb;
  }
}
