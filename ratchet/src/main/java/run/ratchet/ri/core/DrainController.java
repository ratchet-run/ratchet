package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Controls the drain mode state for the job scheduler node, enabling graceful shutdown and
 * maintenance operations. When drain mode is activated, the node stops accepting new jobs while
 * allowing currently executing jobs to complete normally.
 *
 * <p>Drain mode is essential for:
 *
 * <ul>
 *   <li><b>Graceful Shutdown:</b> Ensures no job loss during node shutdown
 *   <li><b>Rolling Updates:</b> Enables zero-downtime deployments
 *   <li><b>Maintenance Windows:</b> Temporarily stops job processing without disrupting in-flight
 *       work
 *   <li><b>Load Balancing:</b> Facilitates removing nodes from the cluster
 * </ul>
 *
 * <p>Thread Safety: All operations are thread-safe and can be called concurrently.
 *
 * @see Poller for drain mode enforcement during job acquisition
 */
@ApplicationScoped
public class DrainController {

  private static final Logger log = Logger.getLogger(DrainController.class.getName());

  private final AtomicBoolean draining = new AtomicBoolean(false);

  /**
   * Checks whether this node is currently in drain mode.
   *
   * @return {@code true} if the node is in drain mode and not accepting new jobs
   */
  public boolean isDraining() {
    return draining.get();
  }

  /**
   * Sets the node's drain mode state.
   *
   * @param value {@code true} to enable drain mode (stop accepting new jobs), {@code false} to
   *     disable drain mode (resume normal operation)
   */
  public void setDraining(boolean value) {
    if (draining.compareAndSet(!value, value)) {
      log.info(value ? "Node set to DRAIN mode" : "Node resumed");
    }
  }
}
