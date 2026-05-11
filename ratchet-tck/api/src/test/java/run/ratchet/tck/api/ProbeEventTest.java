package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ProbeEventTest {

  @Test
  void equalEventsHaveEqualHashCodes() {
    Instant timestamp = Instant.parse("2026-05-07T12:34:56Z");

    ProbeEvent first = new ProbeEvent(ProbeEvent.Type.STARTED, timestamp);
    ProbeEvent second = new ProbeEvent(ProbeEvent.Type.STARTED, timestamp);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void eventsWithDifferentTypesAreNotEqual() {
    Instant timestamp = Instant.parse("2026-05-07T12:34:56Z");

    ProbeEvent started = new ProbeEvent(ProbeEvent.Type.STARTED, timestamp);
    ProbeEvent completed = new ProbeEvent(ProbeEvent.Type.COMPLETED, timestamp);

    assertNotEquals(started, completed);
  }

  @Test
  void eventsWithDifferentTimestampsAreNotEqual() {
    ProbeEvent first =
        new ProbeEvent(ProbeEvent.Type.STARTED, Instant.parse("2026-05-07T12:34:56Z"));
    ProbeEvent second =
        new ProbeEvent(ProbeEvent.Type.STARTED, Instant.parse("2026-05-07T12:34:57Z"));

    assertNotEquals(first, second);
  }

  @Test
  void eventIsNotEqualToNullOrAnotherType() {
    ProbeEvent event =
        new ProbeEvent(ProbeEvent.Type.STARTED, Instant.parse("2026-05-07T12:34:56Z"));

    assertFalse(event.equals(null));
    assertFalse(event.equals("started"));
  }
}
