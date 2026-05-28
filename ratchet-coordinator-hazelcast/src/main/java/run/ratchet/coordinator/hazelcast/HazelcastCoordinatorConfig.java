package run.ratchet.coordinator.hazelcast;

import java.util.Objects;
import java.util.Optional;

/**
 * Tunable configuration for {@link HazelcastClusterCoordinator}.
 *
 * <p>The topic name defaults to {@code "ratchet-wakeup"}. {@code cellId} is appended to it so
 * multi-cell deployments sharing one Hazelcast cluster isolate wakeup traffic on separate topics.
 *
 * @param topicName name of the Hazelcast {@code ITopic} the coordinator publishes / subscribes on
 * @param cellId optional per-cell suffix appended to {@link #topicName}
 * @param maxInboundPayloadChars hard cap on the character length of an inbound topic payload before
 *     the codec rejects it as malformed. Wakeup envelopes are ~80 chars; the default 16384 leaves
 *     three orders of magnitude of headroom for future fields while bounding malformed JSON.
 * @param listenerExecutorThreads worker threads dispatching inbound wakeups to registered
 *     listeners. Default 2 — keeps one slow listener from stalling all others without burning
 *     threads on a 1-listener install.
 * @param shutdownGraceMs max wait for listener removal on close. Default 5000.
 */
public record HazelcastCoordinatorConfig(
    String topicName,
    Optional<String> cellId,
    int maxInboundPayloadChars,
    int listenerExecutorThreads,
    long shutdownGraceMs) {

  public static final String DEFAULT_TOPIC_NAME = "ratchet-wakeup";

  public HazelcastCoordinatorConfig {
    Objects.requireNonNull(topicName, "topicName");
    if (topicName.isBlank()) {
      throw new IllegalArgumentException("topicName must be non-blank");
    }
    Objects.requireNonNull(cellId, "cellId");
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

  public static HazelcastCoordinatorConfig defaults() {
    return new HazelcastCoordinatorConfig(DEFAULT_TOPIC_NAME, Optional.empty(), 16_384, 2, 5_000L);
  }

  /** Effective topic name after applying the optional {@code cellId} suffix. */
  public String effectiveTopicName() {
    return cellId.map(c -> topicName + "-" + c).orElse(topicName);
  }
}
