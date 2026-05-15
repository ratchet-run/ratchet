package run.ratchet.api.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;

class EventApiContractTest {

  private static final UUID JOB_ID = UUID.fromString("018f0000-0000-7000-8000-000000000001");
  private static final UUID NEXT_JOB_ID = UUID.fromString("018f0000-0000-7000-8000-000000000002");
  private static final Instant TIMESTAMP = Instant.parse("2026-05-10T12:00:00Z");

  @Test
  void countAndAttemptAccessorsExposePrimitiveValues() throws NoSuchMethodException {
    assertEquals(int.class, BatchCompletedEvent.class.getMethod("getTotalItems").getReturnType());
    assertEquals(
        int.class, BatchCompletedEvent.class.getMethod("getCompletedItems").getReturnType());
    assertEquals(int.class, BatchCompletedEvent.class.getMethod("getFailedItems").getReturnType());
    assertEquals(int.class, BatchCompletingEvent.class.getMethod("getTotalItems").getReturnType());
    assertEquals(
        int.class, BatchCompletingEvent.class.getMethod("getCompletedItems").getReturnType());
    assertEquals(int.class, BatchCompletingEvent.class.getMethod("getFailedItems").getReturnType());
    assertEquals(
        int.class, JobCallbackFailedEvent.class.getMethod("getCallbackAttempt").getReturnType());
    assertEquals(int.class, JobDlqEvent.class.getMethod("getRetryAttempt").getReturnType());
    assertEquals(int.class, JobFailedEvent.class.getMethod("getRetryAttempt").getReturnType());
    assertEquals(int.class, JobRetryingEvent.class.getMethod("getRetryAttempt").getReturnType());
  }

  @Test
  void jobDlqEventPreservesExplicitTimestamp() {
    JobDlqEvent event =
        new JobDlqEvent(
            JOB_ID,
            "business-key",
            JobType.SINGLE,
            JobPriority.NORMAL,
            "node-a",
            TIMESTAMP,
            "failed",
            3);

    assertEquals(TIMESTAMP, event.getTimestamp());
    assertEquals("failed", event.getErrorMessage());
    assertEquals(3, event.getRetryAttempt());
  }

  @Test
  void signalOutcomeMustBeExplicitWhenUsingFullConstructor() {
    assertThrows(
        NullPointerException.class,
        () ->
            new JobSignaledEvent(
                JOB_ID,
                "business-key",
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                null,
                "operator",
                SignalDecision.Outcome.APPROVED,
                null));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobSignaledEvent(
                JOB_ID,
                "business-key",
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "signal-key",
                null,
                SignalDecision.Outcome.APPROVED,
                null));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobSignaledEvent(
                JOB_ID,
                "business-key",
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "signal-key",
                "operator",
                null,
                null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobSignaledEvent(
                JOB_ID,
                "business-key",
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "signal-key",
                "operator",
                SignalDecision.Outcome.APPROVED,
                "not allowed"));

    JobSignaledEvent event =
        new JobSignaledEvent(
            JOB_ID,
            "business-key",
            JobType.SINGLE,
            JobPriority.NORMAL,
            "node-a",
            "signal-key",
            "operator",
            SignalDecision.Outcome.REJECTED,
            " no ");

    assertEquals(SignalDecision.Outcome.REJECTED, event.getOutcome());
    assertEquals("no", event.getRejectionReason());
  }

  @Test
  void bulkSignalEventNormalizesOutcomeAndRejectionReason() {
    assertThrows(
        NullPointerException.class,
        () ->
            new JobsBulkSignaledEvent(
                null, 2, "operator", SignalDecision.Outcome.APPROVED, null, TIMESTAMP));
    assertThrows(
        NullPointerException.class,
        () -> new JobsBulkSignaledEvent("signal-key", 2, "operator", null, null, TIMESTAMP));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobsBulkSignaledEvent(
                "signal-key", 2, null, SignalDecision.Outcome.APPROVED, null, TIMESTAMP));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobsBulkSignaledEvent(
                "signal-key", 2, "operator", SignalDecision.Outcome.APPROVED, null, null));

    JobsBulkSignaledEvent event =
        new JobsBulkSignaledEvent(
            "signal-key", 2, "operator", SignalDecision.Outcome.REJECTED, " no ", TIMESTAMP);

    assertEquals("signal-key", event.getSignalKey());
    assertEquals(2, event.getCount());
    assertEquals("operator", event.getSignalDeliveredBy());
    assertEquals(SignalDecision.Outcome.REJECTED, event.getOutcome());
    assertEquals("no", event.getRejectionReason());
    assertEquals(TIMESTAMP, event.getSignaledAt());
  }

  @Test
  void performanceMetricsDefensivelyCopiesInputMap() {
    Map<String, Object> data = new HashMap<>();
    data.put("queued", 1);

    PerformanceMetricsEvent event = new PerformanceMetricsEvent(data);
    data.put("running", 2);

    assertFalse(event.performanceData().containsKey("running"));
    assertThrows(
        UnsupportedOperationException.class, () -> event.performanceData().put("failed", 3));
  }

  @Test
  void workflowBranchNextJobIdUsesUuidContract() throws Exception {
    assertEquals(
        UUID.class, WorkflowBranchTriggeredEvent.class.getMethod("getNextJobId").getReturnType());

    Constructor<WorkflowBranchTriggeredEvent> constructor =
        WorkflowBranchTriggeredEvent.class.getConstructor(
            UUID.class,
            String.class,
            JobType.class,
            JobPriority.class,
            String.class,
            Instant.class,
            String.class,
            UUID.class);

    WorkflowBranchTriggeredEvent event =
        constructor.newInstance(
            JOB_ID,
            "business-key",
            JobType.WORKFLOW,
            JobPriority.HIGH,
            "node-a",
            TIMESTAMP,
            "result == true",
            NEXT_JOB_ID);

    assertEquals(NEXT_JOB_ID, event.getNextJobId());
  }
}
