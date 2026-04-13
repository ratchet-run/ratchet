package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/** Controls drain mode for graceful shutdown. */
@ApplicationScoped
public class DrainController {

  private static final Logger log = Logger.getLogger(DrainController.class);

  private final AtomicBoolean draining = new AtomicBoolean(false);

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
