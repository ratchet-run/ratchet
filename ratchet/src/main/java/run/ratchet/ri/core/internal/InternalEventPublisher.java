/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core.internal;

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
