package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.*;

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
