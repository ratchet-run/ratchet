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
package run.ratchet.api.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

class JobCancellationEventTest {

  private static final UUID JOB_ID = UUID.fromString("018f0000-0000-7000-8000-000000000001");
  private static final Instant TIMESTAMP = Instant.parse("2026-05-07T12:34:56Z");

  @Test
  void cancelledEventPreservesExplicitConstructorValues() {
    JobCancelledEvent event =
        new JobCancelledEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.HIGH,
            "node-a",
            TIMESTAMP,
            "RUNNING",
            123L);

    assertCancellationValues(event);
  }

  @Test
  void cancelledShortConstructorMapsAllCancellationFields() {
    Instant before = Instant.now();
    JobCancelledEvent event =
        new JobCancelledEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.HIGH,
            "node-a",
            "PENDING",
            7L);
    Instant after = Instant.now();

    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-key", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals("PENDING", event.getPreviousStatus());
    assertEquals(7L, event.getExecutionTimeMs());
    assertFalse(event.getTimestamp().isBefore(before));
    assertFalse(event.getTimestamp().isAfter(after));
  }

  private static void assertCancellationValues(AbstractJobCancellationEvent event) {
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-key", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals(TIMESTAMP, event.getTimestamp());
    assertEquals("RUNNING", event.getPreviousStatus());
    assertEquals(123L, event.getExecutionTimeMs());
  }
}
