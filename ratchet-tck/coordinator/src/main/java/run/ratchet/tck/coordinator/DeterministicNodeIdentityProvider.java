package run.ratchet.tck.coordinator;

import java.util.Objects;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * Fixed-id {@link NodeIdentityProvider} for deterministic TCK tests.
 *
 * <p>The production provider manages heartbeats and {@code NodeStore} registration; the TCK does
 * not exercise that path — coordinators only consume {@link NodeIdentityProvider#getNodeId()}.
 * Using a constant id keeps fixture identities stable across test runs so self-suppression and
 * envelope-round-trip assertions can compare to a known value.
 */
public final class DeterministicNodeIdentityProvider implements NodeIdentityProvider {

  private final String nodeId;

  public DeterministicNodeIdentityProvider(String nodeId) {
    this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
    if (nodeId.isBlank()) {
      throw new IllegalArgumentException("blank nodeId");
    }
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }
}
