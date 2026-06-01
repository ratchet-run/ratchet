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
package run.ratchet.coordinator.jms;

import java.util.Objects;
import java.util.Optional;

/**
 * Tunable configuration for {@link JmsClusterCoordinator}.
 *
 * <p>JNDI names default to the Jakarta EE platform defaults so a deployment with a default
 * connection factory and a {@code java:comp/Ratchet/Wakeup} topic binding gets push wakeups with
 * zero application configuration. {@code brokerSideSelfFilter} controls whether a {@code node <>
 * '<localId>'} JMS message selector is installed on the consumer to drop self-broadcasts at the
 * broker before they reach the client — saves bandwidth on most brokers, but receive-side
 * self-suppression is always on as defense-in-depth.
 *
 * <p>{@code cellId}, when present, is appended to the topic JNDI name's tail segment so multi-cell
 * deployments on a shared broker isolate wakeup traffic via distinct physical topics. Operators
 * must still bind the per-cell topic resource separately; this record does not provision
 * destinations.
 *
 * @param connectionFactoryJndi optional override of {@code java:comp/DefaultJMSConnectionFactory}.
 * @param topicJndi optional override of {@code java:comp/Ratchet/Wakeup}.
 * @param cellId optional per-cell suffix appended to the topic identifier for operator hygiene.
 * @param brokerSideSelfFilter when true, the consumer is created with a JMS selector that drops
 *     self-broadcasts at the broker. Default true.
 * @param reconnectBackoffInitialMs delay before the first reconnect attempt after an {@code
 *     ExceptionListener} firing. Default 200.
 * @param reconnectBackoffMaxMs cap on the doubled reconnect delay. Default 30000.
 * @param maxInboundPayloadChars hard cap on the character length of an inbound {@code TextMessage}
 *     body before the codec rejects it as malformed. Wakeup envelopes are ~80 chars; the default
 *     16384 leaves three orders of magnitude of headroom for future fields while still bounding the
 *     listener-thread allocation a hostile or buggy producer could trigger.
 * @param listenerExecutorThreads worker threads dispatching inbound wakeups to registered
 *     listeners. Default 2 — keeps one slow listener from stalling all others.
 * @param listenerExecutorQueueCapacity bound on the dispatch pool's pending-task queue. When the
 *     bound is hit the oldest queued wakeup is discarded (wakeups are advisory and a fresher one
 *     supersedes a stale one). Default 1024; replaces the unbounded queue that risked OOM under
 *     sustained wakeup pressure.
 * @param shutdownGraceMs max wait for in-flight callbacks to drain on close. Default 5000.
 */
public record JmsCoordinatorConfig(
    Optional<String> connectionFactoryJndi,
    Optional<String> topicJndi,
    Optional<String> cellId,
    boolean brokerSideSelfFilter,
    long reconnectBackoffInitialMs,
    long reconnectBackoffMaxMs,
    int maxInboundPayloadChars,
    int listenerExecutorThreads,
    int listenerExecutorQueueCapacity,
    long shutdownGraceMs) {

  public static final String DEFAULT_CONNECTION_FACTORY_JNDI =
      "java:comp/DefaultJMSConnectionFactory";
  public static final String DEFAULT_TOPIC_JNDI = "java:comp/Ratchet/Wakeup";

  public JmsCoordinatorConfig {
    Objects.requireNonNull(connectionFactoryJndi, "connectionFactoryJndi");
    Objects.requireNonNull(topicJndi, "topicJndi");
    Objects.requireNonNull(cellId, "cellId");
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
    if (listenerExecutorQueueCapacity < 1) {
      throw new IllegalArgumentException("listenerExecutorQueueCapacity must be >= 1");
    }
    if (shutdownGraceMs <= 0) {
      throw new IllegalArgumentException("shutdownGraceMs must be > 0");
    }
  }

  /** Default tuning suitable for typical Jakarta EE deployments. */
  public static JmsCoordinatorConfig defaults() {
    return new JmsCoordinatorConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        true,
        200L,
        30_000L,
        16_384,
        2,
        1_024,
        5_000L);
  }

  /** Effective connection factory JNDI name after applying the override. */
  public String effectiveConnectionFactoryJndi() {
    return connectionFactoryJndi.orElse(DEFAULT_CONNECTION_FACTORY_JNDI);
  }

  /** Effective topic JNDI name after applying the override. */
  public String effectiveTopicJndi() {
    return topicJndi.orElse(DEFAULT_TOPIC_JNDI);
  }
}
