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
package run.ratchet.store.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

class OutboxEventTest {
  @Test
  void requiredFieldsAndRetryAttemptAreValidated() {
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    assertThrows(
        NullPointerException.class, () -> event(null, OutboxEventType.JOB_FAILED, id, now, now, 0));
    assertThrows(NullPointerException.class, () -> event(id, null, id, now, now, 0));
    assertThrows(
        NullPointerException.class, () -> event(id, OutboxEventType.JOB_FAILED, null, now, now, 0));
    assertThrows(
        NullPointerException.class, () -> event(id, OutboxEventType.JOB_FAILED, id, null, now, 0));
    assertThrows(
        NullPointerException.class, () -> event(id, OutboxEventType.JOB_FAILED, id, now, null, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> event(id, OutboxEventType.JOB_FAILED, id, now, now, -1));
  }

  @Test
  void completionPlanSnapshotsEventsAndNormalizesNull() {
    var events = new ArrayList<OutboxEvent>();
    var event =
        event(
            UUID.randomUUID(),
            OutboxEventType.JOB_COMPLETED,
            UUID.randomUUID(),
            Instant.now(),
            Instant.now(),
            0);
    events.add(event);
    var plan = plan(events);
    events.clear();
    assertEquals(List.of(event), plan.outboxEvents());
    assertThrows(UnsupportedOperationException.class, () -> plan.outboxEvents().clear());
    assertEquals(List.of(), plan(null).outboxEvents());
    assertThrows(NullPointerException.class, () -> plan(Arrays.asList((OutboxEvent) null)));
  }

  private static OutboxEvent event(
      UUID eventId,
      OutboxEventType type,
      UUID jobId,
      Instant timestamp,
      Instant created,
      int retry) {
    return new OutboxEvent(
        eventId, type, jobId, null, null, null, null, null, timestamp, null, null, retry, created);
  }

  private static JobCompletionPlan plan(List<OutboxEvent> events) {
    return new JobCompletionPlan(
        UUID.randomUUID(),
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
        null,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        List.of(),
        null,
        events);
  }
}
