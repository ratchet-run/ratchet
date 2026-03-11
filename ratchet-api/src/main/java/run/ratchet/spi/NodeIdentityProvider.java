package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Provides the identity of the current cluster node. */
@Incubating
public interface NodeIdentityProvider {

  String getNodeId();
}
