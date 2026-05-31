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

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import run.ratchet.spi.PayloadSerializer;

/**
 * Static holder that routes JPA {@link jakarta.persistence.AttributeConverter} JSON I/O through the
 * framework's {@link PayloadSerializer} SPI.
 *
 * <p><b>Rationale:</b> JPA attribute converters are instantiated by the persistence provider, not
 * by CDI, so they cannot use {@code @Inject} to obtain the container-managed {@link
 * PayloadSerializer}. Wiring through a static holder preserves the SPI contract: at container
 * startup the reference-implementation producer calls {@link #set(PayloadSerializer)} with the
 * discovered CDI bean; converter {@code convertTo…} calls then delegate to that SPI. When no
 * serializer has been set (non-CDI environments: raw unit tests, pre-deployment schema tools), the
 * holder falls back to a locally-constructed JSON-B instance so converters remain usable.
 *
 * <p>Lives in {@code store-core} (not {@code ratchet}) because the converters themselves live here,
 * and keeping the fallback close to its consumer avoids a hard dependency from store-core on the
 * reference implementation.
 */
public final class PayloadSerializerHolder {

  private static volatile PayloadSerializer delegate;

  private static volatile Jsonb fallbackJsonb;

  private PayloadSerializerHolder() {}

  /**
   * Installs the framework-managed {@link PayloadSerializer} used by converters. Called once at
   * container startup by the reference implementation's producer.
   *
   * @param serializer the serializer to install; MAY be {@code null} to revert to the fallback
   */
  public static void set(PayloadSerializer serializer) {
    delegate = serializer;
  }

  /**
   * Returns the currently-installed {@link PayloadSerializer} or a lazy fallback using a
   * locally-constructed JSON-B instance if none has been registered.
   */
  public static PayloadSerializer get() {
    PayloadSerializer current = delegate;
    if (current != null) {
      return current;
    }
    return FallbackHolder.INSTANCE;
  }

  /**
   * Returns the lazily-created fallback {@link Jsonb} instance, constructing it on first use.
   *
   * <p>The fallback instance is intentionally held for the lifetime of the JVM process and is never
   * closed. {@code Jsonb} implements {@link AutoCloseable}, but closing a JVM-lifetime singleton
   * would leak its {@link jakarta.json.bind.JsonbConfig} and provider resources to GC without any
   * practical benefit in a long-running server process. In OSGi or repeated test-class scenarios,
   * the CDI path ({@link #set(PayloadSerializer)}) should be used instead of triggering the
   * fallback.
   */
  private static Jsonb fallbackJsonb() {
    // Double-checked locking keeps the common converter path out of the synchronized block.
    Jsonb jsonb = fallbackJsonb;
    if (jsonb == null) {
      synchronized (PayloadSerializerHolder.class) {
        if (fallbackJsonb == null) {
          fallbackJsonb = JsonbBuilder.create();
        }
        jsonb = fallbackJsonb;
      }
    }
    return jsonb;
  }

  private static final class FallbackHolder {

    static final PayloadSerializer INSTANCE =
        new PayloadSerializer() {
          @Override
          public String serialize(Object payload) {
            if (payload == null) {
              return null;
            }
            try {
              return fallbackJsonb().toJson(payload);
            } catch (JsonbException e) {
              throw new IllegalArgumentException(
                  "JSON-B fallback serialization error for " + payload.getClass().getName(), e);
            }
          }

          @Override
          public <T> T deserialize(String json, Class<T> type) {
            if (json == null || json.isEmpty()) {
              return null;
            }
            try {
              return fallbackJsonb().fromJson(json, type);
            } catch (JsonbException e) {
              throw new IllegalArgumentException(
                  "JSON-B fallback deserialization error for "
                      + (type == null ? "null" : type.getName()),
                  e);
            }
          }
        };
  }
}
