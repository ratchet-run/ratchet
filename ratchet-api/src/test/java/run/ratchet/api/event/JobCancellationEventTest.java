package run.ratchet.api.event;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    JobCancelledEvent event =
        new JobCancelledEvent(
            JOB_ID,
            "business-key",
            JobType.SINGLE,
            JobPriority.HIGH,
            "node-a",
            TIMESTAMP,
            "PENDING",
            7L);

    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-key", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals("PENDING", event.getPreviousStatus());
    assertEquals(7L, event.getExecutionTimeMs());
    assertEquals(TIMESTAMP, event.getTimestamp());
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
