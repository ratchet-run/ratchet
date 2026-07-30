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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serial;
import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.event.JobSignaledEvent;
import run.ratchet.api.event.JobsBulkCancelledEvent;
import run.ratchet.api.event.JobsBulkSignaledEvent;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobSchedulerServiceSignalTest {

  private static final UUID JOB_ID = new UUID(0L, 88L);
  private static final Instant FIXED_NOW = Instant.parse("2026-05-05T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private InternalEventPublisher eventPublisher;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobPauseStore jobPauseStore;
  @Mock private JobRetryStore jobRetryStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private JobWakeupService wakeupService;
  @Mock private run.ratchet.store.spi.RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private DefaultJobCreationService jobCreationService;
  @Mock private JobAuthorizationPolicy authorizationPolicy;
  @Mock private SignalStore signalStore;
  @Mock private PayloadSerializer payloadSerializer;
  @Mock private MetricsCollector metricsCollector;

  private DefaultJobSchedulerService service;

  private record RawSignalPayload(String status, int score) implements Serializable {
    @Serial private static final long serialVersionUID = 1L;
  }

  private static JobEntity job(UUID id, String signalKey) {
    JobEntity job = new JobEntity();
    job.setId(id);
    job.setBusinessKey("business-" + id.getLeastSignificantBits());
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setSignalKey(signalKey);
    return job;
  }

  @BeforeEach
  void setUp() {
    service = newService(payloadSerializer);
  }

  @Test
  void deliverSignalApprovedDecisionByIdPersistsNullRejectionReasonAndPublishesEvent() {
    SignalDecision decision = SignalDecision.approved("approved-payload");
    JobEntity job = job(JOB_ID, "approval-key");
    when(payloadSerializer.serialize("approved-payload")).thenReturn("serialized-payload");
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("APPROVED"),
            isNull(),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(1);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));

    assertEquals(1, service.deliverSignal(JOB_ID, decision));

    verify(metricsCollector)
        .signalDelivered(
            JOB_ID, job.getPublicJobType(), "approval-key", SignalDecision.Outcome.APPROVED);
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobSignaledEvent event = assertInstanceOf(JobSignaledEvent.class, eventCaptor.getValue());
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("approval-key", event.getSignalKey());
    assertEquals("bob", event.getSignalDeliveredBy());
    assertEquals(SignalDecision.Outcome.APPROVED, event.getOutcome());
    assertNull(event.getRejectionReason());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }

  @Test
  void deliverSignalDecisionByIdPersistsDecisionMetadataAndPublishesEvent() {
    SignalDecision decision = SignalDecision.rejected("needs-review", "denied");
    JobEntity job = job(JOB_ID, "approval-key");
    when(payloadSerializer.serialize("needs-review")).thenReturn("serialized-payload");
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("REJECTED"),
            eq("denied"),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(1);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));

    assertEquals(1, service.deliverSignal(JOB_ID, decision));

    verify(metricsCollector)
        .signalDelivered(
            JOB_ID, job.getPublicJobType(), "approval-key", SignalDecision.Outcome.REJECTED);
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobSignaledEvent event = (JobSignaledEvent) eventCaptor.getValue();
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("approval-key", event.getSignalKey());
    assertEquals("bob", event.getSignalDeliveredBy());
    assertEquals(SignalDecision.Outcome.REJECTED, event.getOutcome());
    assertEquals("denied", event.getRejectionReason());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }

  @Test
  void deliverSignalByIdPublishesFromPreCasSnapshotWhenPostCasReloadWouldMiss() {
    DefaultJobSchedulerService authorizedService =
        newService(payloadSerializer, authorizationPolicy);
    SignalDecision decision = SignalDecision.approved("approved-payload");
    JobEntity job = job(JOB_ID, "approval-key");
    job.setCallerPrincipal("alice");
    when(payloadSerializer.serialize("approved-payload")).thenReturn("serialized-payload");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job), Optional.empty());
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("APPROVED"),
            isNull(),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(1);

    assertEquals(1, authorizedService.deliverSignal(JOB_ID, decision));

    verify(authorizationPolicy).checkDeliverSignal(JOB_ID, "alice", "bob");
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobSignaledEvent event = assertInstanceOf(JobSignaledEvent.class, eventCaptor.getValue());
    assertEquals("approval-key", event.getSignalKey());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }

  @Test
  void deliverSignalRawPayloadByKeyPersistsRawMetadataAndPublishesBulkEvent() {
    RawSignalPayload payload = new RawSignalPayload("ready", 7);
    AtomicReference<String> deliveryId = new AtomicReference<>();
    when(payloadSerializer.serialize(payload)).thenReturn("serialized-raw-payload");
    when(signalStore.deliverSignalByKey(
            eq("approval-key"),
            eq("serialized-raw-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_RAW),
            eq("APPROVED"),
            isNull(),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenAnswer(
            inv -> {
              deliveryId.set(inv.getArgument(7, String.class));
              return 2;
            });

    assertEquals(2, service.deliverSignal("approval-key", payload));

    verify(signalStore, never()).findJobsBySignalDeliveryId(deliveryId.get());
    verify(metricsCollector, never()).signalDelivered(any(), any(), anyString(), any());

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobsBulkSignaledEvent event =
        assertInstanceOf(JobsBulkSignaledEvent.class, eventCaptor.getValue());
    assertEquals("approval-key", event.getSignalKey());
    assertEquals(2, event.getCount());
    assertEquals("bob", event.getSignalDeliveredBy());
    assertEquals(SignalDecision.Outcome.APPROVED, event.getOutcome());
    assertNull(event.getRejectionReason());
    assertEquals(FIXED_NOW, event.getSignaledAt());
  }

  @Test
  void deliverSignalDecisionByKeyPublishesBulkEventForBulkDelivery() {
    SignalDecision decision = SignalDecision.rejected("needs-review", "nope");
    AtomicReference<String> deliveryId = new AtomicReference<>();
    when(payloadSerializer.serialize("needs-review")).thenReturn("serialized-payload");
    when(signalStore.deliverSignalByKey(
            eq("approval-key"),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("REJECTED"),
            eq("nope"),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenAnswer(
            inv -> {
              deliveryId.set(inv.getArgument(7, String.class));
              return 2;
            });

    assertEquals(2, service.deliverSignal("approval-key", decision));

    verify(signalStore, never()).findJobsBySignalDeliveryId(deliveryId.get());
    verify(metricsCollector, never()).signalDelivered(any(), any(), anyString(), any());

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobsBulkSignaledEvent event =
        assertInstanceOf(JobsBulkSignaledEvent.class, eventCaptor.getValue());
    assertEquals("approval-key", event.getSignalKey());
    assertEquals(2, event.getCount());
    assertEquals("bob", event.getSignalDeliveredBy());
    assertEquals(SignalDecision.Outcome.REJECTED, event.getOutcome());
    assertEquals("nope", event.getRejectionReason());
    assertEquals(FIXED_NOW, event.getSignaledAt());
  }

  @Test
  void deliverSignalByIdChecksAuthorizationBeforeStoreUpdate() {
    DefaultJobSchedulerService authorizedService =
        newService(payloadSerializer, authorizationPolicy);
    JobEntity job = job(JOB_ID, "approval-key");
    job.setCallerPrincipal("alice");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            isNull(),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_RAW),
            eq("APPROVED"),
            isNull(),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(1);

    assertEquals(1, authorizedService.deliverSignal(JOB_ID, (Serializable) null));

    verify(authorizationPolicy).checkDeliverSignal(JOB_ID, "alice", "bob");
    verify(signalStore)
        .deliverSignalById(
            eq(JOB_ID), isNull(), eq("RAW"), eq("APPROVED"), isNull(), eq("bob"), any(), any());
  }

  @Test
  void deliverSignalByIdAuthorizationDenialSkipsStoreUpdate() {
    DefaultJobSchedulerService authorizedService =
        newService(payloadSerializer, authorizationPolicy);
    JobEntity job = job(JOB_ID, "approval-key");
    job.setCallerPrincipal("alice");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    doThrow(new JobAuthorizationException(JOB_ID, "deliverSignal", "bob", "denied"))
        .when(authorizationPolicy)
        .checkDeliverSignal(JOB_ID, "alice", "bob");

    assertThrows(
        JobAuthorizationException.class,
        () -> authorizedService.deliverSignal(JOB_ID, (Serializable) null));

    verify(signalStore, never())
        .deliverSignalById(eq(JOB_ID), any(), any(), any(), any(), any(), any(), any());
    verify(eventPublisher, never()).publish(any());
  }

  @Test
  void deliverSignalByKeyWithNullPrincipalPersistsAndPublishesSystemActor() {
    DefaultJobSchedulerService systemService =
        newService(payloadSerializer, metricsCollector, authorizationPolicy, Optional.empty());
    when(signalStore.deliverSignalByKey(
            eq("approval-key"),
            isNull(),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_RAW),
            eq("APPROVED"),
            isNull(),
            eq("system"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(2);

    assertEquals(2, systemService.deliverSignal("approval-key", (Serializable) null));

    verify(authorizationPolicy).checkDeliverSignal("approval-key", null);
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobsBulkSignaledEvent event =
        assertInstanceOf(JobsBulkSignaledEvent.class, eventCaptor.getValue());
    assertEquals("system", event.getSignalDeliveredBy());
  }

  @Test
  void deliverSignalRawWithPayloadWithoutSerializerThrowsBeforeStoreUpdate() {
    DefaultJobSchedulerService noSerializer = newService(null);

    assertThrows(IllegalStateException.class, () -> noSerializer.deliverSignal(JOB_ID, "payload"));

    verify(signalStore, never())
        .deliverSignalById(eq(JOB_ID), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void deliverSignalRawByKeyWithoutSerializerThrowsBeforeStoreUpdate() {
    DefaultJobSchedulerService noSerializer = newService(null);

    assertThrows(
        IllegalStateException.class, () -> noSerializer.deliverSignal("approval-key", "payload"));

    verify(signalStore, never())
        .deliverSignalByKey(eq("approval-key"), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void deliverSignalWithoutMetricsCollectorStillPublishesEvent() {
    DefaultJobSchedulerService noMetrics = newService(payloadSerializer, (MetricsCollector) null);
    SignalDecision decision = SignalDecision.approved("approved-payload");
    JobEntity job = job(JOB_ID, "approval-key");
    when(payloadSerializer.serialize("approved-payload")).thenReturn("serialized-payload");
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("APPROVED"),
            isNull(),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(1);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));

    assertEquals(1, noMetrics.deliverSignal(JOB_ID, decision));

    verify(eventPublisher).publish(any(JobSignaledEvent.class));
  }

  @Test
  void deliverSignalDecisionByIdReturnsZeroWithoutEventOrMetricsWhenNoRowsUnblocked() {
    SignalDecision decision = SignalDecision.approved("approved-payload");
    when(payloadSerializer.serialize("approved-payload")).thenReturn("serialized-payload");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job(JOB_ID, "approval-key")));
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("APPROVED"),
            isNull(),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(0);

    assertEquals(0, service.deliverSignal(JOB_ID, decision));

    verify(eventPublisher, never()).publish(any());
    verify(metricsCollector, never()).signalDelivered(any(), any(), anyString(), any());
  }

  @Test
  void deliverSignalDecisionByKeyReturnsZeroWithoutEventWhenNoRowsUnblocked() {
    SignalDecision decision = SignalDecision.rejected("needs-review", "nope");
    when(payloadSerializer.serialize("needs-review")).thenReturn("serialized-payload");
    when(signalStore.deliverSignalByKey(
            eq("approval-key"),
            eq("serialized-payload"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("REJECTED"),
            eq("nope"),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenReturn(0);

    assertEquals(0, service.deliverSignal("approval-key", decision));

    verify(eventPublisher, never()).publish(any());
    verify(metricsCollector, never()).signalDelivered(any(), any(), anyString(), any());
  }

  @Test
  void cancelJobsByTagPublishesBulkEventUsingInjectedClock() {
    when(jobBatchStatusStore.cancelJobsByTag("stale")).thenReturn(3);

    assertEquals(3, service.cancelJobsByTag("stale"));

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobsBulkCancelledEvent event = (JobsBulkCancelledEvent) eventCaptor.getValue();
    assertEquals("stale", event.getTag());
    assertEquals(3, event.getCount());
    assertEquals(FIXED_NOW, event.getCancelledAt());
  }

  private DefaultJobSchedulerService newService(PayloadSerializer serializer) {
    return newService(serializer, metricsCollector);
  }

  private DefaultJobSchedulerService newService(
      PayloadSerializer serializer, MetricsCollector signalMetricsCollector) {
    return newService(serializer, signalMetricsCollector, null, Optional.of("bob"));
  }

  private DefaultJobSchedulerService newService(
      PayloadSerializer serializer, JobAuthorizationPolicy signalAuthorizationPolicy) {
    return newService(serializer, metricsCollector, signalAuthorizationPolicy, Optional.of("bob"));
  }

  private DefaultJobSchedulerService newService(
      PayloadSerializer serializer,
      MetricsCollector signalMetricsCollector,
      JobAuthorizationPolicy signalAuthorizationPolicy,
      Optional<String> currentPrincipal) {
    CallerPrincipalProvider callerProvider =
        new CallerPrincipalProvider(null) {
          @Override
          public Optional<String> currentPrincipal() {
            return currentPrincipal;
          }
        };

    return new DefaultJobSchedulerService(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        null,
        jobCreationService,
        callerProvider,
        signalAuthorizationPolicy,
        signalStore,
        serializer,
        signalMetricsCollector,
        FIXED_CLOCK,
        new StubAfterCommitRegistrar());
  }
}
