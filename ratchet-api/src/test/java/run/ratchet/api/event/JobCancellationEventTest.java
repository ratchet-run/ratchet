package run.ratchet.api.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

class JobCancellationEventTest {

  private static final UUID JOB_ID = UUID.fromString("018f0000-0000-7000-8000-000000000001");
  private static final Instant TIMESTAMP = Instant.parse("2026-05-07T12:34:56Z");

  @Test
  void cancellingEventPreservesExplicitConstructorValues() {
    JobCancellingEvent event =
        new JobCancellingEvent(
            JOB_ID,
            "business-key",
            JobType.SINGLE,
            JobPriority.HIGH,
            "node-a",
            TIMESTAMP,
            "RUNNING",
            123L);

    assertCancellationValues(event);
    assertInstanceOf(AbstractJobCancellationEvent.class, event);
    assertInstanceOf(AbstractJobSchedulerEvent.class, event);
  }

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
    assertInstanceOf(AbstractJobCancellationEvent.class, event);
    assertInstanceOf(AbstractJobSchedulerEvent.class, event);
  }

  @Test
  void cancellingDefaultTimestampConstructorStillInitializesSharedCancellationValues() {
    Instant before = Instant.now();
    JobCancellingEvent event =
        new JobCancellingEvent(
            JOB_ID, "business-key", JobType.SINGLE, JobPriority.HIGH, "node-a", "PENDING", 7L);
    Instant after = Instant.now();

    assertDefaultTimestampConstructorValues(event, before, after);
  }

  @Test
  void cancelledDefaultTimestampConstructorStillInitializesSharedCancellationValues() {
    Instant before = Instant.now();
    JobCancelledEvent event =
        new JobCancelledEvent(
            JOB_ID, "business-key", JobType.SINGLE, JobPriority.HIGH, "node-a", "PENDING", 7L);
    Instant after = Instant.now();

    assertDefaultTimestampConstructorValues(event, before, after);
  }

  private static void assertDefaultTimestampConstructorValues(
      AbstractJobCancellationEvent event, Instant before, Instant after) {
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-key", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals("PENDING", event.getPreviousStatus());
    assertEquals(7L, event.getExecutionTimeMs());
    assertNotNull(event.getTimestamp());
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
