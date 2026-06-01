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
package run.ratchet.coordinator.infinispan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class InfinispanCoordinatorConfigTest {

  @Test
  void defaultsProduceValidConfig() {
    InfinispanCoordinatorConfig c = InfinispanCoordinatorConfig.defaults();
    assertNotNull(c);
    assertEquals(InfinispanCoordinatorConfig.DEFAULT_CACHE_NAME, c.cacheName());
    assertTrue(c.cellId().isEmpty());
    assertTrue(c.wakeupTtlSeconds() > 0);
    assertEquals(16_384, c.maxInboundPayloadChars());
    assertTrue(c.shutdownGraceMs() > 0);
  }

  @Test
  void effectiveCacheNameAppliesCellId() {
    InfinispanCoordinatorConfig c =
        new InfinispanCoordinatorConfig(
            "base", Optional.of("cellA"), 60L, 16_384, 2, 1_024, 5_000L);
    assertEquals("base_cellA", c.effectiveCacheName());
  }

  @Test
  void nonPositiveWakeupTtlRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new InfinispanCoordinatorConfig(
                "wakeup", Optional.empty(), 0L, 16_384, 2, 1_024, 5_000L));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new InfinispanCoordinatorConfig(
                "wakeup", Optional.empty(), -1L, 16_384, 2, 1_024, 5_000L));
  }

  @Test
  void nonPositiveShutdownGraceRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 60L, 16_384, 2, 1_024, 0L));
  }

  @Test
  void blankCacheNameRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InfinispanCoordinatorConfig("", Optional.empty(), 60L, 16_384, 2, 1_024, 5_000L));
  }

  @Test
  void nonPositiveListenerThreadsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new InfinispanCoordinatorConfig(
                "wakeup", Optional.empty(), 60L, 16_384, 0, 1_024, 5_000L));
  }

  @Test
  void nonPositiveMaxInboundPayloadCharsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 60L, 0, 2, 1_024, 5_000L));
  }
}
