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
 * (registered via {@link #addListener}) and a container event bridge.
 *
 * <p>A programmatic listener that throws is logged and suppressed so the remaining listeners still
 * run. A container-bridge exception is logged and suppressed too, but only after that bridge's
 * synchronous dispatch may already have aborted its remaining observers for the event. Either way,
 * callers must not rely on {@link #publish(Object)} to roll back their transaction when an observer
 * fails.
 */
@ApplicationScoped
public class InternalEventPublisher {
  private static final Logger log = Logger.getLogger(InternalEventPublisher.class);
  private final List<Consumer<Object>> listeners = new CopyOnWriteArrayList<>();
  private final Consumer<Object> containerBridge;

  protected InternalEventPublisher() {
    this((Consumer<Object>) null);
  }

  @Inject
  public InternalEventPublisher(Event<Object> cdiEvent) {
    this(cdiEvent == null ? null : cdiEvent::fire);
  }

  public InternalEventPublisher(Consumer<Object> containerBridge) {
    this.containerBridge = containerBridge;
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

    if (containerBridge != null) {
      try {
        containerBridge.accept(event);
      } catch (Exception e) {
        log.warnf(e, "Container event bridge threw exception for event: %s", eventType(event));
      }
    }
  }

  private static String eventType(Object event) {
    return event == null ? "<null>" : event.getClass().getSimpleName();
  }
}
