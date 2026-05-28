package run.ratchet.tck.coordinator;

import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.ClusterCoordinator;

/**
 * Two-node cluster fixture handed to each TCK test by the {@link CoordinatorTestHarness}.
 *
 * <p>The fixture owns nothing about the transport itself — it borrows two coordinator instances
 * from the harness, exposes the {@link NodeIdentity} each carries, and exposes a {@link
 * RecordingMetricsCollector} per node so per-side assertions remain unambiguous. Closing the
 * fixture closes both coordinators in {@code A → B} order; harness-level transport teardown is the
 * caller's responsibility (driven by {@link CoordinatorTestHarness#close()}).
 *
 * <p>{@link #close()} swallows close-time exceptions — TCK teardown should never mask the original
 * test failure with a teardown-side error.
 */
public record CoordinatorTestFixture(
    ClusterCoordinator nodeA,
    NodeIdentity identityA,
    ClusterCoordinator nodeB,
    NodeIdentity identityB,
    RecordingMetricsCollector metricsA,
    RecordingMetricsCollector metricsB)
    implements AutoCloseable {

  @Override
  public void close() {
    closeQuietly(nodeA);
    closeQuietly(nodeB);
  }

  private static void closeQuietly(ClusterCoordinator c) {
    if (c == null) {
      return;
    }
    try {
      c.close();
    } catch (RuntimeException ignored) {
      // Idempotent close is required by the SPI; tolerate misbehavior so teardown stays loud-free.
    }
  }
}
