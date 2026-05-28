package run.ratchet.tck.coordinator;

import java.time.Duration;
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
