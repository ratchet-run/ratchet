package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.DrainController;

/** Controls drain mode for graceful shutdown. */
@ApplicationScoped
public class DefaultDrainController implements DrainController {

  private static final Logger log = Logger.getLogger(DefaultDrainController.class);

  private final AtomicBoolean draining = new AtomicBoolean(false);

  @Override
  public boolean isDraining() {
    return draining.get();
  }

  @Override
  public void setDraining(boolean value) {
    if (draining.compareAndSet(!value, value)) {
      log.info(value ? "Node set to DRAIN mode" : "Node resumed");
    }
  }
}
