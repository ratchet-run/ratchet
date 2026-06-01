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
package run.ratchet.testsuite.app;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.spi.PayloadSerializer;

/**
 * CDI {@link Alternative} {@link PayloadSerializer} that counts every framework invocation and
 * otherwise delegates to JSON-B. Used by {@code CustomSerializationStrategyIT} to prove that the
 * framework routes payload and result JSON through the SPI during real job execution (not just
 * bean-producibility — the counter increments only when the framework itself invokes the SPI).
 *
 * <p>The invocation counters are static because Arquillian tests may observe them from the test
 * runner side of the deployment boundary. Each test that deploys this bean must call {@link
 * #resetCounts()} before enqueueing jobs so counts remain scoped to that test method.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class CountingPayloadSerializer implements PayloadSerializer {

  private static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger(0);

  private volatile Jsonb jsonb;

  public static int getSerializeCount() {
    return SERIALIZE_COUNT.get();
  }

  public static int getDeserializeCount() {
    return DESERIALIZE_COUNT.get();
  }

  /** Resets test-visible static counters; call from {@code @BeforeEach} before scheduling jobs. */
  public static void resetCounts() {
    SERIALIZE_COUNT.set(0);
    DESERIALIZE_COUNT.set(0);
  }

  @Override
  public String serialize(Object payload) {
    SERIALIZE_COUNT.incrementAndGet();
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
    DESERIALIZE_COUNT.incrementAndGet();
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
    this.jsonb = JsonbBuilder.create();
  }

  @PreDestroy
  void close() {
    Jsonb instance = this.jsonb;
    if (instance != null) {
      try {
        instance.close();
      } catch (Exception ignored) {
        // Best-effort cleanup during test deployment shutdown.
      }
    }
  }

  private Jsonb jsonb() {
    Jsonb instance = this.jsonb;
    if (instance == null) {
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
