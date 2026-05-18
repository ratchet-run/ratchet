package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Returns the stable, unique identifier for this scheduler node. Used for heartbeats, job claiming,
 * and cluster coordination.
 *
 * @since 0.1
 */
@Incubating
public interface NodeIdentityProvider {

  /** Returns the non-null node identifier. Must be immutable for the provider lifecycle. */
  String getNodeId();
}
