package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
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

  @BeforeEach
  void setUp() {
    service = newService(payloadSerializer);
  }

  private DefaultJobSchedulerService newService(PayloadSerializer serializer) {
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
        metricsCollector,
        FIXED_CLOCK);
  }

  @Test
  void deliverSignalDecisionByIdPersistsDecisionMetadataAndPublishesEvent() {
    SignalDecision decision = SignalDecision.rejected("needs-review", "denied");
    JobEntity job = job(JOB_ID, "approval-key");
    when(payloadSerializer.serialize(decision)).thenReturn("serialized-decision");
    when(signalStore.deliverSignalById(
            eq(JOB_ID),
            eq("serialized-decision"),
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
  void deliverSignalDecisionByKeyPublishesPerJobEventsForBulkDelivery() {
    SignalDecision decision = SignalDecision.rejected("needs-review", "nope");
    JobEntity j1 = job(new UUID(0L, 1L), "approval-key");
    JobEntity j2 = job(new UUID(0L, 2L), "approval-key");
    AtomicReference<String> deliveryId = new AtomicReference<>();
    when(payloadSerializer.serialize(decision)).thenReturn("serialized-decision");
    when(signalStore.deliverSignalByKey(
            eq("approval-key"),
            eq("serialized-decision"),
            eq(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION),
            eq("REJECTED"),
            eq("nope"),
            eq("bob"),
            eq(FIXED_NOW),
            anyString()))
        .thenAnswer(
            inv -> {
              deliveryId.set(inv.getArgument(7, String.class));
              when(signalStore.findJobsBySignalDeliveryId(deliveryId.get()))
                  .thenReturn(List.of(j1, j2));
              return 2;
            });

    assertEquals(2, service.deliverSignal("approval-key", decision));

    verify(signalStore).findJobsBySignalDeliveryId(deliveryId.get());
    verify(metricsCollector)
        .signalDelivered(
            j1.getId(), j1.getPublicJobType(), "approval-key", SignalDecision.Outcome.REJECTED);
    verify(metricsCollector)
        .signalDelivered(
            j2.getId(), j2.getPublicJobType(), "approval-key", SignalDecision.Outcome.REJECTED);

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, org.mockito.Mockito.times(2)).publish(eventCaptor.capture());
    List<JobSignaledEvent> events =
        eventCaptor.getAllValues().stream().map(JobSignaledEvent.class::cast).toList();
    assertEquals(
        List.of(j1.getId(), j2.getId()), events.stream().map(JobSignaledEvent::getJobId).toList());
    assertEquals(
        List.of(SignalDecision.Outcome.REJECTED, SignalDecision.Outcome.REJECTED),
        events.stream().map(JobSignaledEvent::getOutcome).toList());
  }

  @Test
  void deliverSignalRawWithPayloadWithoutSerializerThrowsBeforeStoreUpdate() {
    DefaultJobSchedulerService noSerializer = newService(null);

    assertThrows(IllegalStateException.class, () -> noSerializer.deliverSignal(JOB_ID, "payload"));

    verify(signalStore, never())
        .deliverSignalById(eq(JOB_ID), any(), any(), any(), any(), any(), any(), any());
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
}
