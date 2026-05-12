package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * Synchronous event publisher for internal RI use. Fires events to both programmatic listeners
 * (registered via {@link #addListener}) and CDI observers (via {@link Event#fire}).
 *
 * <p>Listener and CDI observer failures are logged and suppressed so one broken observer does not
 * stop later observers. Callers must not rely on {@link #publish(Object)} to roll back their
 * transaction when an observer fails.
 */
@ApplicationScoped
public class InternalEventPublisher {
  private static final Logger log = Logger.getLogger(InternalEventPublisher.class);
  private final List<Consumer<Object>> listeners = new CopyOnWriteArrayList<>();
  private final Event<Object> cdiEvent;

  protected InternalEventPublisher() {
    this.cdiEvent = null;
  }

  @Inject
  public InternalEventPublisher(Event<Object> cdiEvent) {
    this.cdiEvent = cdiEvent;
  }

  public void addListener(Consumer<Object> listener) {
    listeners.add(listener);
  }

  public void removeListener(Consumer<Object> listener) {
    listeners.remove(listener);
  }

  /**
   * Publishes an event synchronously. Observer failures are non-fatal; see class Javadoc for the
   * failure contract.
   */
  public void publish(Object event) {
    for (Consumer<Object> listener : listeners) {
      try {
        listener.accept(event);
      } catch (Exception e) {
        log.warnf(e, "Event listener threw exception for event: %s", eventType(event));
      }
    }

    if (cdiEvent != null) {
      try {
        cdiEvent.fire(event);
      } catch (Exception e) {
        log.warnf(e, "CDI event fire threw exception for event: %s", eventType(event));
      }
    }
  }

  private static String eventType(Object event) {
    return event == null ? "<null>" : event.getClass().getSimpleName();
  }
}
