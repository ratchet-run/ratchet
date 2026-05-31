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

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Tunable configuration for {@link PostgresqlListenNotifyCoordinator}.
 *
 * <p>The channel name is a PostgreSQL identifier (quoted at use, no SQL injection risk). When
 * {@code cellId} is present, the effective channel becomes {@code channel + "_" + cellId} so
 * multi-cell deployments sharing a PG cluster can isolate wakeup traffic.
 *
 * <p>{@code receiveTimeoutMs} controls only the LISTEN loop wait. The coordinator keeps the LISTEN
 * connection single-purpose and borrows separate publish connections for {@code pg_notify}, so this
 * timeout does not bound outbound send latency.
 *
 * @param channel base NOTIFY channel name; default {@code "ratchet_wakeup"}
 * @param cellId optional per-cell suffix appended to {@code channel}
 * @param receiveTimeoutMs upper bound on {@code getNotifications} wait per iteration. Default 1000.
 * @param reconnectBackoffInitialMs delay applied after the first failed reconnect attempt; the
 *     first attempt fires immediately. Each subsequent failure doubles the delay. Default 200.
 * @param reconnectBackoffMaxMs cap on the doubled reconnect delay. Default 30000.
 * @param maxInboundPayloadChars hard cap on the character length of an inbound NOTIFY payload
 *     before the codec rejects it as malformed. Wakeup envelopes are ~80 chars; the default 16384
 *     leaves three orders of magnitude of headroom for future fields while bounding malformed JSON.
 * @param listenerExecutorThreads worker threads for dispatching to registered wakeup listeners.
 *     Default 1.
 * @param shutdownGraceMs max wait for the listen thread and listener executor to drain on close.
 *     Default 5000.
 */
public record PostgresqlCoordinatorConfig(
    String channel,
    Optional<String> cellId,
    long receiveTimeoutMs,
    long reconnectBackoffInitialMs,
    long reconnectBackoffMaxMs,
    int maxInboundPayloadChars,
    int listenerExecutorThreads,
    long shutdownGraceMs) {

  public static final String DEFAULT_CHANNEL = "ratchet_wakeup";

  /**
   * PostgreSQL identifier limit per {@code NAMEDATALEN} (default build, 64): channel names longer
   * than 63 bytes get silently truncated server-side, so two deployments sharing the same 63-byte
   * prefix would merge wakeup traffic.
   */
  static final int MAX_CHANNEL_BYTES = 63;

  /**
   * PostgreSQL-friendly identifier charset: starts with a letter or underscore, then letters,
   * digits, or underscores. The pattern applies to both {@code channel} and {@code cellId}
   * independently because they are concatenated to form the effective channel; embedded control
   * characters in either would produce confusing errors at NOTIFY execution rather than at
   * configuration time.
   */
  static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  public PostgresqlCoordinatorConfig {
    Objects.requireNonNull(channel, "channel");
    if (channel.isBlank()) {
      throw new IllegalArgumentException("channel must be non-blank");
    }
    if (!IDENTIFIER_PATTERN.matcher(channel).matches()) {
      throw new IllegalArgumentException(
          "channel '" + channel + "' must match " + IDENTIFIER_PATTERN.pattern());
    }
    Objects.requireNonNull(cellId, "cellId");
    cellId.ifPresent(
        c -> {
          if (!IDENTIFIER_PATTERN.matcher(c).matches()) {
            throw new IllegalArgumentException(
                "cellId '" + c + "' must match " + IDENTIFIER_PATTERN.pattern());
          }
        });
    String effective = cellId.map(c -> channel + "_" + c).orElse(channel);
    int effectiveBytes = effective.getBytes(StandardCharsets.UTF_8).length;
    if (effectiveBytes > MAX_CHANNEL_BYTES) {
      throw new IllegalArgumentException(
          "effective channel '"
              + effective
              + "' is "
              + effectiveBytes
              + " bytes; PostgreSQL truncates identifiers to "
              + MAX_CHANNEL_BYTES
              + " bytes which would silently merge wakeup traffic across deployments");
    }
    if (receiveTimeoutMs <= 0) {
      throw new IllegalArgumentException("receiveTimeoutMs must be > 0");
    }
    if (reconnectBackoffInitialMs <= 0) {
      throw new IllegalArgumentException("reconnectBackoffInitialMs must be > 0");
    }
    if (reconnectBackoffMaxMs < reconnectBackoffInitialMs) {
      throw new IllegalArgumentException(
          "reconnectBackoffMaxMs must be >= reconnectBackoffInitialMs");
    }
    if (maxInboundPayloadChars <= 0) {
      throw new IllegalArgumentException("maxInboundPayloadChars must be > 0");
    }
    if (listenerExecutorThreads < 1) {
      throw new IllegalArgumentException("listenerExecutorThreads must be >= 1");
    }
    if (shutdownGraceMs <= 0) {
      throw new IllegalArgumentException("shutdownGraceMs must be > 0");
    }
  }

  /** Default tuning suitable for typical deployments. */
  public static PostgresqlCoordinatorConfig defaults() {
    return new PostgresqlCoordinatorConfig(
        DEFAULT_CHANNEL, Optional.empty(), 1_000L, 200L, 30_000L, 16_384, 1, 5_000L);
  }

  /** The fully-qualified channel name after applying the optional {@code cellId} suffix. */
  public String effectiveChannel() {
    return cellId.map(c -> channel + "_" + c).orElse(channel);
  }
}
