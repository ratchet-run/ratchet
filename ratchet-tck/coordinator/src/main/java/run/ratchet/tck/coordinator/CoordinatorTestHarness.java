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

import java.time.Duration;
import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.ClusterCoordinator;

/**
 * Per-implementation SPI the abstract coordinator contracts call into to provision and tear down a
 * two-node transport.
 *
 * <p>Implementations decide how to provision the shared transport — Testcontainers PostgreSQL,
 * embedded Artemis, two cache managers, two members — and how to model "transport failure" for the
 * resilience tests (drop the connection, stop the broker, partition the cluster, etc.).
 *
 * <p>Harness lifecycle is per-test by default ({@code twoNodeCluster()} from {@code @BeforeEach}).
 * Implementations that want to amortize container startup should reuse the underlying container at
 * the class level (typical Testcontainers pattern) and provision fresh coordinator instances per
 * call instead.
 */
public interface CoordinatorTestHarness extends AutoCloseable {

  /**
   * Construct two isolated coordinators that can communicate over the same transport. Returns a
   * fixture exposing both coordinators, their deterministic {@link run.ratchet.api.NodeIdentity
   * NodeIdentity}s, and per-side metrics recorders.
   */
  CoordinatorTestFixture twoNodeCluster() throws Exception;

  /**
   * Force the underlying transport into a failed state for transport-failure tests.
   *
   * <p>Implementations interpret "fail" appropriately: drop the connection, stop the broker, stop a
   * cache-manager transport, stop a member's instance, etc. After {@link #recoverTransport()}
   * delivery must resume.
   */
  void forceTransportFailure() throws Exception;

  /** Restore the transport so subsequent operations can succeed. */
  void recoverTransport() throws Exception;

  /**
   * Estimated upper bound on transport latency for assertion deadlines. Implementations that ship a
   * fast embedded transport may return a tighter bound; defaults to 5 seconds to absorb container
   * scheduling jitter on CI.
   */
  default Duration maxExpectedLatency() {
    return Duration.ofSeconds(5);
  }

  /**
   * Whether the harness can inject a raw wire payload bypassing the codec — used by the
   * unknown-envelope-version test. Defaults to {@code false}; implementations that can drive the
   * transport from outside the coordinator's API (e.g. PostgreSQL via {@code psql NOTIFY}) override
   * this and {@link #injectRawMessage}.
   */
  default boolean supportsRawWireInjection() {
    return false;
  }

  /**
   * Inject a raw wire message addressed at the receiving coordinator. Only invoked when {@link
   * #supportsRawWireInjection()} returns {@code true}. The default throws to make accidental calls
   * loud.
   */
  default void injectRawMessage(ClusterCoordinator receiver, String rawPayload) throws Exception {
    throw new UnsupportedOperationException(
        "harness does not support raw wire injection — see supportsRawWireInjection()");
  }

  /**
   * Returns a raw wire message, addressed from {@code source}, carrying an envelope version far
   * beyond any this coordinator's codec will ever support. The unknown-envelope-version contract
   * injects it and expects the receiver to reject it loudly.
   *
   * <p>This keeps the concrete wire schema out of the abstract contract: a JSON coordinator emits a
   * bumped JSON envelope, a protobuf coordinator emits a bumped protobuf frame, a JMS-object
   * coordinator emits its own form. Only invoked when {@link #supportsRawWireInjection()} returns
   * {@code true}; the default throws to make accidental calls loud.
   *
   * @param source the originating node identity to stamp into the envelope
   * @return the raw, transport-ready payload string
   */
  default String futureVersionRawMessage(NodeIdentity source) {
    throw new UnsupportedOperationException(
        "harness does not support raw wire injection — see supportsRawWireInjection()");
  }

  /**
   * Whether a single coordinator instance can survive a transport failure + recovery cycle without
   * being reconstructed. PostgreSQL and JMS coordinators both reconnect transparently; Infinispan
   * and Hazelcast bind to a provider-owned cache-manager / member instance whose restart
   * invalidates the coordinator's transport reference (per PRD §Reconnect / cluster churn). The
   * recovery contract test gates on this flag.
   */
  default boolean transportRecoverableWithinCoordinatorLifetime() {
    return true;
  }

  /** Tear down per-harness transport resources. Implementations make this idempotent. */
  @Override
  void close() throws Exception;
}
