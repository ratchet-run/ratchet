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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;

class EventApiContractTest {

  private static final UUID JOB_ID = UUID.fromString("018f0000-0000-7000-8000-000000000001");
  private static final UUID NEXT_JOB_ID = UUID.fromString("018f0000-0000-7000-8000-000000000002");
  private static final UUID RECURRING_MASTER_ID =
      UUID.fromString("018f0000-0000-7000-8000-000000000003");
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
    assertEquals(
        int.class, JobExecutionTimedOutEvent.class.getMethod("getRetryAttempt").getReturnType());
    assertEquals(int.class, JobFailedEvent.class.getMethod("getRetryAttempt").getReturnType());
    assertEquals(int.class, JobRetryingEvent.class.getMethod("getRetryAttempt").getReturnType());
  }

  @Test
  void jobExecutionTimedOutEventPreservesTimeoutContext() {
    JobExecutionTimedOutEvent event =
        new JobExecutionTimedOutEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.HIGH,
            "node-a",
            TIMESTAMP,
            Duration.ofSeconds(30),
            Duration.ofSeconds(31),
            2);

    assertEquals(TIMESTAMP, event.getTimestamp());
    assertEquals(Duration.ofSeconds(30), event.getExecutionTimeout());
    assertEquals(Duration.ofSeconds(31), event.getElapsedTime());
    assertEquals(2, event.getRetryAttempt());
  }

  @Test
  void jobDlqEventPreservesExplicitTimestamp() {
    JobDlqEvent event =
        new JobDlqEvent(
            JOB_ID,
            "business-key",
            null,
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
  void perJobEventsRequireJobIdAndTimestamp() {
    assertThrows(
        NullPointerException.class,
        () ->
            new JobStartedEvent(
                null, "business-key", null, JobType.SINGLE, JobPriority.NORMAL, null));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobStartedEvent(
                JOB_ID, "business-key", null, JobType.SINGLE, JobPriority.NORMAL, null, null));

    JobStartedEvent event =
        new JobStartedEvent(JOB_ID, null, RECURRING_MASTER_ID, null, null, null, TIMESTAMP);

    assertEquals(JOB_ID, event.getJobId());
    assertEquals(RECURRING_MASTER_ID, event.getRecurringMasterId());
    assertEquals(TIMESTAMP, event.getTimestamp());
  }

  @Test
  void signalEventsPreserveExplicitTimestamps() {
    JobSignalWaitingEvent waiting =
        new JobSignalWaitingEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.NORMAL,
            "node-a",
            TIMESTAMP,
            "signal-key",
            Duration.ofMinutes(5));
    JobSignalTimedOutEvent timedOut =
        new JobSignalTimedOutEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.NORMAL,
            "node-a",
            TIMESTAMP,
            "signal-key",
            Duration.ofMinutes(5));
    JobSignaledEvent signaled =
        new JobSignaledEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.NORMAL,
            "node-a",
            TIMESTAMP,
            "signal-key",
            "operator",
            SignalDecision.Outcome.APPROVED,
            null);

    assertEquals(TIMESTAMP, waiting.getTimestamp());
    assertEquals(TIMESTAMP, timedOut.getTimestamp());
    assertEquals(TIMESTAMP, signaled.getTimestamp());
  }

  @Test
  void eventSpecificRequiredFieldsAreValidated() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ChainStartedEvent(
                JOB_ID, "business-key", null, JobType.CHAIN, JobPriority.NORMAL, "node-a", null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ChainFailedEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.CHAIN,
                JobPriority.NORMAL,
                "node-a",
                NEXT_JOB_ID,
                " "));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowBranchTriggeredEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.WORKFLOW,
                JobPriority.NORMAL,
                "node-a",
                " ",
                NEXT_JOB_ID));
    assertThrows(
        NullPointerException.class,
        () ->
            new WorkflowBranchTriggeredEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.WORKFLOW,
                JobPriority.NORMAL,
                "node-a",
                "result == true",
                null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobSignalWaitingEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                " ",
                null));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobSignalTimedOutEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "signal-key",
                null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobExecutionTimedOutEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                Duration.ZERO,
                Duration.ofSeconds(1),
                1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobExecutionTimedOutEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                Duration.ofSeconds(1),
                Duration.ofMillis(-1),
                1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobExecutionTimedOutEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobCancelledEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                " ",
                null));
  }

  @Test
  void countsAttemptsAndDurationsRejectInvalidRanges() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BatchCompletedEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.BATCH,
                JobPriority.NORMAL,
                "node-a",
                0,
                0,
                0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BatchCompletingEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.BATCH,
                JobPriority.NORMAL,
                "node-a",
                3,
                2,
                2));
    JobFailedEvent firstAttemptFailure =
        new JobFailedEvent(
            JOB_ID,
            "business-key",
            null,
            JobType.SINGLE,
            JobPriority.NORMAL,
            "node-a",
            "failed",
            0);
    assertEquals(0, firstAttemptFailure.getRetryAttempt());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobFailedEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "failed",
                -1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobCallbackFailedEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                JobCallbackFailedEvent.CallbackType.ON_FAILURE,
                "failed",
                "java.lang.IllegalStateException",
                0));
    assertThrows(
        NullPointerException.class,
        () ->
            new JobRetryingEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "failed",
                1,
                null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobCompletedEvent(
                JOB_ID, "business-key", null, JobType.SINGLE, JobPriority.NORMAL, "node-a", -1L));
    assertThrows(
        IllegalArgumentException.class, () -> new JobsBulkCancelledEvent("tag-a", 0, TIMESTAMP));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobsBulkSignaledEvent(
                "signal-key", 0, "operator", SignalDecision.Outcome.APPROVED, null, TIMESTAMP));
  }

  @Test
  void signalOutcomeMustBeExplicitWhenUsingFullConstructor() {
    assertThrows(
        NullPointerException.class,
        () ->
            new JobSignaledEvent(
                JOB_ID,
                "business-key",
                null,
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
                null,
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
                null,
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
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                "signal-key",
                "operator",
                SignalDecision.Outcome.APPROVED,
                "not allowed"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobSignaledEvent(
                JOB_ID,
                "business-key",
                null,
                JobType.SINGLE,
                JobPriority.NORMAL,
                "node-a",
                " ",
                "operator",
                SignalDecision.Outcome.APPROVED,
                null));

    JobSignaledEvent event =
        new JobSignaledEvent(
            JOB_ID,
            "business-key",
            null,
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
  void bulkRetriedEventValidatesBoundsAndPreservesSelection() {
    JobFilter filter = JobFilter.builder().tags("billing").build();
    JobsBulkRetriedEvent event = new JobsBulkRetriedEvent(filter, 25, 7, TIMESTAMP);

    assertEquals(filter, event.getFilter());
    assertEquals(25, event.getLimit());
    assertEquals(7, event.getCount());
    assertEquals(TIMESTAMP, event.getRetriedAt());
    assertThrows(
        IllegalArgumentException.class, () -> new JobsBulkRetriedEvent(filter, 0, 1, TIMESTAMP));
    assertThrows(
        IllegalArgumentException.class, () -> new JobsBulkRetriedEvent(filter, 1, 2, TIMESTAMP));
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
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JobsBulkSignaledEvent(
                "signal-key",
                2,
                "operator",
                SignalDecision.Outcome.APPROVED,
                "not allowed",
                TIMESTAMP));

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
  void workflowBranchNextJobIdUsesUuidContract() throws Exception {
    assertEquals(
        UUID.class, WorkflowBranchTriggeredEvent.class.getMethod("getNextJobId").getReturnType());

    Constructor<WorkflowBranchTriggeredEvent> constructor =
        WorkflowBranchTriggeredEvent.class.getConstructor(
            UUID.class,
            String.class,
            UUID.class,
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
            null,
            JobType.WORKFLOW,
            JobPriority.HIGH,
            "node-a",
            TIMESTAMP,
            "result == true",
            NEXT_JOB_ID);

    assertEquals(NEXT_JOB_ID, event.getNextJobId());
  }
}
