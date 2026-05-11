package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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
import run.ratchet.ri.security.CallerPrincipalProvider;
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
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private DefaultJobCreationService jobCreationService;
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
    DefaultJobSchedulerService noMetrics = newService(payloadSerializer, null);
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
    CallerPrincipalProvider callerProvider =
        new CallerPrincipalProvider(null) {
          @Override
          public Optional<String> currentPrincipal() {
            return Optional.of("bob");
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
        wakeupService,
        recurringScheduler,
        null,
        jobCreationService,
        callerProvider,
        null,
        signalStore,
        serializer,
        signalMetricsCollector,
        FIXED_CLOCK);
  }
}
