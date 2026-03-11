package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronous event publisher for internal RI use. Fires events to both programmatic listeners
 * (registered via {@link #addListener}) and CDI observers (via {@link Event#fire}).
 */
@ApplicationScoped
public class InternalEventPublisher {
  private static final Logger log = Logger.getLogger(InternalEventPublisher.class.getName());
  private final List<Consumer<Object>> listeners = new CopyOnWriteArrayList<>();

  @Inject private Event<Object> cdiEvent;

  public void addListener(Consumer<Object> listener) {
    listeners.add(listener);
  }

  public void removeListener(Consumer<Object> listener) {
    listeners.remove(listener);
  }

  public void publish(Object event) {
    // Fire to programmatic listeners
    for (Consumer<Object> listener : listeners) {
      try {
        listener.accept(event);
      } catch (Exception e) {
        log.log(
            Level.WARNING,
            "Event listener threw exception for event: " + event.getClass().getSimpleName(),
            e);
      }
    }

    // Fire to CDI observers
    if (cdiEvent != null) {
      try {
        cdiEvent.fire(event);
      } catch (Exception e) {
        log.log(
            Level.WARNING,
            "CDI event fire threw exception for event: " + event.getClass().getSimpleName(),
            e);
      }
    }
  }
}
