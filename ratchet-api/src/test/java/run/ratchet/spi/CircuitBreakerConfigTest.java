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
package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CircuitBreakerConfigTest {

  @Test
  void acceptsValidValues() {
    CircuitBreakerConfig config = new CircuitBreakerConfig(50.0f, 10, 1000L, 2, 5);

    assertEquals(50.0f, config.failureRateThreshold());
    assertEquals(10, config.slidingWindowSize());
    assertEquals(1000L, config.waitDurationMs());
    assertEquals(2, config.permittedCallsInHalfOpen());
    assertEquals(5, config.minimumCalls());
    assertEquals(CircuitBreakerExceptionFilter.RECORD_ALL, config.exceptionFilter());
  }

  @Test
  void nullExceptionListsAreNormalized() {
    CircuitBreakerConfig config =
        new CircuitBreakerConfig(50.0f, 10, 1000L, 2, 5, null, null, null);
    assertEquals(List.of(), config.recordExceptions());
    assertEquals(List.of(), config.ignoreExceptions());
    assertEquals(CircuitBreakerExceptionFilter.RECORD_ALL, config.exceptionFilter());
  }

  @Test
  void exceptionListsAreImmutableCopies() {
    List<Class<? extends Throwable>> records =
        new ArrayList<>(List.of(IllegalStateException.class));
    List<Class<? extends Throwable>> ignores =
        new ArrayList<>(List.of(IllegalArgumentException.class));
    CircuitBreakerConfig config =
        new CircuitBreakerConfig(50.0f, 10, 1000L, 2, 5, records, ignores, t -> false);
    records.clear();
    ignores.clear();
    assertEquals(List.of(IllegalStateException.class), config.recordExceptions());
    assertEquals(List.of(IllegalArgumentException.class), config.ignoreExceptions());
    assertThrows(UnsupportedOperationException.class, () -> config.recordExceptions().clear());
    assertThrows(UnsupportedOperationException.class, () -> config.ignoreExceptions().clear());
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.RECORDED,
        config.exceptionFilter().classify(new IllegalStateException()));
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.IGNORED,
        config.exceptionFilter().classify(new IllegalArgumentException()));
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.NOT_RECORDED,
        config.exceptionFilter().classify(new Exception()));
    records.add(null);
    assertThrows(
        NullPointerException.class,
        () -> new CircuitBreakerConfig(50.0f, 10, 1000L, 2, 5, records, List.of(), null));
    assertThrows(
        NullPointerException.class,
        () -> new CircuitBreakerConfig(50.0f, 10, 1000L, 2, 5, List.of(), records, null));
  }

  @Test
  void rejectsInvalidValues() {
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(-1.0f, 10, 1000L, 2, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(101.0f, 10, 1000L, 2, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(Float.NaN, 10, 1000L, 2, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(50.0f, 0, 1000L, 2, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(50.0f, 10, -1L, 2, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(50.0f, 10, 1000L, 0, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfig(50.0f, 10, 1000L, 2, 0));
  }
}
