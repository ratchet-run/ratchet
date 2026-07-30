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
  void failedAndDlqEventsRemainDistinct() {
    Instant timestamp = Instant.parse("2026-05-07T12:34:56Z");

    ProbeEvent failed = new ProbeEvent(ProbeEvent.Type.FAILED, timestamp);
    ProbeEvent dlq = new ProbeEvent(ProbeEvent.Type.DLQ, timestamp);

    assertNotEquals(failed, dlq);
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
