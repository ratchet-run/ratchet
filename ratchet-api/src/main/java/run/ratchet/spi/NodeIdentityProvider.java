package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Provides an abstraction for retrieving the unique identifier of a node within the system.
 *
 * <p>This interface is marked as {@link Incubating}, indicating that it is experimental and subject
 * to potential changes in future releases. Implementations of this interface should ensure that the
 * node identifier is immutable and consistent throughout the lifecycle of the application.
 *
 * <p>Node identifiers are typically used for critical operations such as heartbeat signaling, job
 * coordination, and high-availability scenarios where each node must be uniquely distinguishable.
 *
 * <p>Implementations must be thread-safe to ensure that the identifier can be safely accessed
 * concurrently in multi-threaded environments.
 */
@Incubating
public interface NodeIdentityProvider {

  /**
   * Retrieves the unique identifier of the node.
   *
   * @return a {@code String} representing the unique identifier of the node. This identifier must
   *     be immutable and consistent for the node's lifecycle.
   */
  String getNodeId();
}
