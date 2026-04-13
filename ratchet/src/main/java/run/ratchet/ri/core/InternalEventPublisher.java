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
 * <p><b>Synchronous dispatch — latency warning.</b> Both programmatic listeners and CDI observers
 * are invoked on the publishing thread, which is often the job execution thread. A slow listener or
 * observer creates unbounded latency on the job hot path and can stall the scheduler. This is by
 * design for transactional consistency — events fire inside the same {@code @Transactional}
 * boundary as the state change they announce, so observers can participate in the same transaction.
 * Listeners that do heavyweight work (I/O, network calls, cross-system notifications) MUST offload
 * to their own thread pool. CDI observers that need async semantics should use
 * {@code @ObservesAsync} instead of {@code @Observes}.
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
   * Publishes an event synchronously to all registered listeners and CDI observers.
   *
   * <p><b>Synchronous:</b> this method runs all listeners and observers on the caller's thread
   * before returning. A slow listener will delay the caller — typically a job worker thread.
   * Listeners that do non-trivial work must dispatch asynchronously internally. See the class
   * Javadoc for the rationale (transactional consistency) and the recommended
   * {@code @ObservesAsync} pattern for heavyweight CDI observers.
   */
  public void publish(Object event) {
    // Fire to programmatic listeners
    for (Consumer<Object> listener : listeners) {
      try {
        listener.accept(event);
      } catch (Exception e) {
        log.warnf(
            e, "Event listener threw exception for event: %s", event.getClass().getSimpleName());
      }
    }

    // Fire to CDI observers
    if (cdiEvent != null) {
      try {
        cdiEvent.fire(event);
      } catch (Exception e) {
        log.warnf(
            e, "CDI event fire threw exception for event: %s", event.getClass().getSimpleName());
      }
    }
  }
}
