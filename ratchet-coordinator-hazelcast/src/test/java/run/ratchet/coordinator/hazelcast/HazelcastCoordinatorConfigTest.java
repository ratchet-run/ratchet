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
package run.ratchet.coordinator.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class HazelcastCoordinatorConfigTest {

  @Test
  void defaultsProduceValidConfig() {
    HazelcastCoordinatorConfig c = HazelcastCoordinatorConfig.defaults();
    assertNotNull(c);
    assertEquals(HazelcastCoordinatorConfig.DEFAULT_TOPIC_NAME, c.topicName());
    assertTrue(c.cellId().isEmpty());
    assertEquals(16_384, c.maxInboundPayloadChars());
    assertTrue(c.shutdownGraceMs() > 0);
  }

  @Test
  void effectiveTopicNameAppliesCellId() {
    HazelcastCoordinatorConfig c =
        new HazelcastCoordinatorConfig("base", Optional.of("tenant42"), 16_384, 2, 1_024, 5_000L);
    assertEquals("base-tenant42", c.effectiveTopicName());
  }

  @Test
  void effectiveTopicNameOmitsCellIdSuffixWhenEmpty() {
    HazelcastCoordinatorConfig c =
        new HazelcastCoordinatorConfig("base", Optional.empty(), 16_384, 2, 1_024, 5_000L);
    assertEquals("base", c.effectiveTopicName());
  }

  @Test
  void blankTopicNameRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("", Optional.empty(), 16_384, 2, 1_024, 5_000L));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("   ", Optional.empty(), 16_384, 2, 1_024, 5_000L));
  }

  @Test
  void nullArgumentsRejected() {
    assertThrows(
        NullPointerException.class,
        () -> new HazelcastCoordinatorConfig(null, Optional.empty(), 16_384, 2, 1_024, 5_000L));
    assertThrows(
        NullPointerException.class,
        () -> new HazelcastCoordinatorConfig("base", null, 16_384, 2, 1_024, 5_000L));
  }

  @Test
  void nonPositiveShutdownGraceRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 16_384, 2, 1_024, 0L));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 16_384, 2, 1_024, -1L));
  }

  @Test
  void nonPositiveListenerThreadsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 16_384, 0, 1_024, 5_000L));
  }

  @Test
  void nonPositiveMaxInboundPayloadCharsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 0, 2, 1_024, 5_000L));
  }
}
