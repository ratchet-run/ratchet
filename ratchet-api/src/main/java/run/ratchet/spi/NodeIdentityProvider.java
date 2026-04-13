package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Returns the stable, unique identifier for this scheduler node. Used for heartbeats, job claiming,
 * and cluster coordination.
 */
@Incubating
public interface NodeIdentityProvider {

  /** Returns the node identifier. Must be immutable for the node's lifecycle. */
  String getNodeId();
}
