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
package run.ratchet.ri.cdi.internal;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import org.jboss.logging.Logger;
import run.ratchet.spi.PayloadSerializer;

/**
 * Default {@link PayloadSerializer} backed by Jakarta JSON Binding (JSON-B).
 *
 * <p>This is the out-of-the-box serializer for payload and result JSON persistence. Holds a single
 * {@link Jsonb} instance per application for thread-safe reuse across the poller, JPA attribute
 * converters, and the result persistence pipeline.
 *
 * <p>Vendor-neutral: does not reference any specific JSON-B provider (Yasson, Johnzon, etc.). The
 * runtime-discovered provider is selected by the host application's classpath/module-path.
 *
 * <p><b>Parse limits:</b> JSON-B 3.0 does not standardize parse-limit configuration. Provider-
 * specific property keys (e.g. Yasson's {@code org.eclipse.yasson.*} properties) are silently
 * ignored on other implementations and cannot be used without sacrificing portability. Applications
 * that require payload size enforcement should validate size at the API boundary before submission
 * rather than inside the serializer. This will be revisited if a future JSON-B release standardizes
 * depth and length limits.
 *
 * @apiNote Internal RI implementation. Applications interact with the {@link PayloadSerializer}
 *     SPI, not this class. Public visibility is retained because a cross-package fallback
 *     constructor in {@code ri.core.JobTask} instantiates this class directly. Not part of the
 *     supported API surface.
 */
@ApplicationScoped
public class JsonbPayloadSerializer implements PayloadSerializer {

  private static final Logger log = Logger.getLogger(JsonbPayloadSerializer.class);

  private volatile Jsonb jsonb;

  @Override
  public String serialize(Object payload) {
    if (payload == null) {
      return null;
    }
    try {
      return jsonb().toJson(payload);
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
      return jsonb().fromJson(json, type);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B deserialization error for " + (type == null ? "null" : type.getName()), e);
    }
  }

  @PostConstruct
  void init() {
    // Reuse a single configured Jsonb for the lifetime of the bean. JSON-B implementations
    // maintain internal caches; recreating per call would be wasteful.
    this.jsonb = JsonbBuilder.create();
  }

  @PreDestroy
  void close() {
    Jsonb instance = this.jsonb;
    if (instance != null) {
      try {
        instance.close();
      } catch (Exception e) {
        log.warnf(e, "Failed to close Jsonb instance during shutdown");
      }
    }
  }

  private Jsonb jsonb() {
    Jsonb instance = this.jsonb;
    if (instance == null) {
      // Fallback for direct-construction paths (tests, non-CDI wiring). Container-managed
      // instances go through @PostConstruct above.
      synchronized (this) {
        if (this.jsonb == null) {
          this.jsonb = JsonbBuilder.create();
        }
        instance = this.jsonb;
      }
    }
    return instance;
  }
}
