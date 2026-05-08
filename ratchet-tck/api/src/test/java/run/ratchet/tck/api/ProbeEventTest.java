package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
