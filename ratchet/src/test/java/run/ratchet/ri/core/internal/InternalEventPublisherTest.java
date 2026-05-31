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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jakarta.enterprise.event.Event;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class InternalEventPublisherTest {

  @Test
  void publishDeliversToAllListeners() {
    InternalEventPublisher publisher = new InternalEventPublisher();
    List<Object> received1 = new ArrayList<>();
    List<Object> received2 = new ArrayList<>();

    publisher.addListener(received1::add);
    publisher.addListener(received2::add);

    publisher.publish("event1");
    publisher.publish("event2");

    assertEquals(List.of("event1", "event2"), received1);
    assertEquals(List.of("event1", "event2"), received2);
  }

  @Test
  void listenerExceptionDoesNotStopOtherListeners() {
    InternalEventPublisher publisher = new InternalEventPublisher();
    List<Object> received = new ArrayList<>();

    publisher.addListener(
        e -> {
          throw new RuntimeException("broken listener");
        });
    publisher.addListener(received::add);

    publisher.publish("event");

    assertEquals(List.of("event"), received);
  }

  @Test
  void publishAlsoFiresCdiEvent() {
    @SuppressWarnings("unchecked")
    Event<Object> cdiEvent = mock(Event.class);
    InternalEventPublisher publisher = new InternalEventPublisher(cdiEvent);

    publisher.publish("event");

    verify(cdiEvent).fire("event");
  }

  @Test
  void cdiEventExceptionDoesNotEscapePublish() {
    @SuppressWarnings("unchecked")
    Event<Object> cdiEvent = mock(Event.class);
    InternalEventPublisher publisher = new InternalEventPublisher(cdiEvent);
    doThrow(new RuntimeException("broken observer")).when(cdiEvent).fire("event");

    assertDoesNotThrow(() -> publisher.publish("event"));
  }

  @Test
  void publishAllowsNullEventWhenListenersAcceptIt() {
    InternalEventPublisher publisher = new InternalEventPublisher();
    List<Object> received = new ArrayList<>();

    publisher.addListener(received::add);

    publisher.publish(null);

    assertEquals(1, received.size());
    assertNull(received.get(0));
  }

  @Test
  void removeListenerStopsDelivery() {
    InternalEventPublisher publisher = new InternalEventPublisher();
    List<Object> received = new ArrayList<>();
    var listener =
        new Consumer<>() {
          @Override
          public void accept(Object o) {
            received.add(o);
          }
        };

    publisher.addListener(listener);
    publisher.publish("event1");
    publisher.removeListener(listener);
    publisher.publish("event2");

    assertEquals(List.of("event1"), received);
  }
}
