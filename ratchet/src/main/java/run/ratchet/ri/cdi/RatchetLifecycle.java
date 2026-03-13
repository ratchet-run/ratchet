package run.ratchet.ri.cdi;

import run.ratchet.ri.core.Poller;
import run.ratchet.ri.core.RecurringScheduler;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.logging.Logger;

/**
 * CDI lifecycle observer that initializes and shuts down the Ratchet job scheduler subsystem.
 *
 * <p>On application startup, this bean eagerly initializes the {@link Poller} and {@link
 * RecurringScheduler}. On shutdown, it stops both to allow graceful termination.
 */
@ApplicationScoped
public class RatchetLifecycle {

  private static final Logger log = Logger.getLogger(RatchetLifecycle.class.getName());

  private final Poller poller;
  private final RecurringScheduler recurringScheduler;

  protected RatchetLifecycle() {
    this.poller = null;
    this.recurringScheduler = null;
  }

  @Inject
  public RatchetLifecycle(Poller poller, RecurringScheduler recurringScheduler) {
    this.poller = poller;
    this.recurringScheduler = recurringScheduler;
  }

  /**
   * Initializes the job scheduler subsystem at application startup.
   *
   * @param init the CDI initialization event
   */
  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
    log.info("Initializing Ratchet job scheduler...");
    poller.init();
    recurringScheduler.init();
  }

  /** Stops the poller and recurring scheduler during application shutdown. */
  @PreDestroy
  void onShutdown() {
    log.info("Shutting down Ratchet job scheduler...");
    poller.stop();
    recurringScheduler.stop();
  }
}
