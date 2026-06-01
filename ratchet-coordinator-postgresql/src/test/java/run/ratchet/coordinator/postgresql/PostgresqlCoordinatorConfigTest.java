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
package run.ratchet.coordinator.postgresql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class PostgresqlCoordinatorConfigTest {

  @Test
  void defaultsProduceValidConfig() {
    PostgresqlCoordinatorConfig c = PostgresqlCoordinatorConfig.defaults();
    assertEquals(PostgresqlCoordinatorConfig.DEFAULT_CHANNEL, c.channel());
    assertTrue(c.cellId().isEmpty());
    assertEquals(16_384, c.maxInboundPayloadChars());
  }

  @Test
  void effectiveChannelExactly63BytesAccepted() {
    String channel = "a".repeat(63);
    assertDoesNotThrow(
        () ->
            new PostgresqlCoordinatorConfig(
                channel, Optional.empty(), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
  }

  @Test
  void effectiveChannelOver63BytesRejected() {
    String channel = "a".repeat(64);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new PostgresqlCoordinatorConfig(
                    channel, Optional.empty(), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
    assertTrue(ex.getMessage().contains("64 bytes"), ex.getMessage());
    assertTrue(ex.getMessage().contains("truncate"), ex.getMessage());
  }

  @Test
  void effectiveChannelWithCellIdSuffixOver63BytesRejected() {
    String channel = "a".repeat(60);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new PostgresqlCoordinatorConfig(
                    channel, Optional.of("cell"), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
    assertTrue(ex.getMessage().contains("65 bytes"), ex.getMessage());
  }

  @Test
  void channelWithControlCharRejected() {
    String channel = "wake" + "\0" + "up";
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PostgresqlCoordinatorConfig(
                channel, Optional.empty(), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
  }

  @Test
  void channelWithNonAsciiRejected() {
    String channel = "wakeup\u00E9";
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PostgresqlCoordinatorConfig(
                channel, Optional.empty(), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
  }

  @Test
  void cellIdWithControlCharRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PostgresqlCoordinatorConfig(
                "wakeup", Optional.of("cell\t1"), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
  }

  @Test
  void channelStartingWithDigitRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PostgresqlCoordinatorConfig(
                "1wakeup", Optional.empty(), 5_000L, 200L, 30_000L, 16_384, 1, 1_024, 5_000L));
  }

  @Test
  void nonPositiveMaxInboundPayloadCharsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PostgresqlCoordinatorConfig(
                "wakeup", Optional.empty(), 5_000L, 200L, 30_000L, 0, 1, 1_024, 5_000L));
  }
}
