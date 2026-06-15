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
package run.ratchet.tck.coordinator;

/**
 * Narrow metrics surface the {@link AbstractClusterCoordinatorContract} relies on for assertions.
 *
 * <p>Each coordinator-module test class supplies an implementation that wraps the metric counters
 * the coordinator under test maintains internally — either by delegating to {@link
 * run.ratchet.spi.MetricsCollector#clusterWakeupPublished} / {@link
 * run.ratchet.spi.MetricsCollector#clusterWakeupReceived} call counts or by reading internal {@link
 * java.util.concurrent.atomic.AtomicLong} counters directly when the implementation has not yet
 * been wired to a real metrics binder.
 *
 * <p>Outcome strings used by the first-party coordinators (and the contract):
 *
 * <ul>
 *   <li>Outbound (published): {@code success}, {@code failure}.
 *   <li>Inbound (received): {@code delivered}, {@code ignored_self}, {@code parse_failure}, {@code
 *       transport_failure}, plus implementation-specific overflow / drop outcomes.
 * </ul>
 *
 * <p>An implementation that does not yet model a counter may return {@code 0} as a stable
 * placeholder — TCK assertions that touch that counter are expressed as {@code >= 0} or {@code > 0}
 * so a not-yet-implemented surface does not silently pass a positive expectation.
 */
public interface RecordingMetricsCollector {

  /** Total successful + failed publishes observed (i.e. all calls to {@code notifyNewWork}). */
  long sent();

  /**
   * Inbound deliveries that arrived AND were dispatched to listeners (outcome={@code delivered}).
   */
  long received();

  /**
   * Inbound deliveries dropped because they originated from the local node ({@code ignored_self}).
   */
  long selfNotifySuppressed();

  /**
   * Outbound or inbound transport-layer failures, including parse failures, that the SPI swallowed.
   */
  long transportFailure();

  /** Listener invocations that threw and were isolated by the coordinator. */
  long listenerFailure();

  /**
   * Inbound deliveries dropped because the pre-registration buffer overflowed before any listener
   * was registered. {@code 0} for coordinators that don't implement a pre-registration buffer.
   */
  long preRegistrationOverflow();
}
