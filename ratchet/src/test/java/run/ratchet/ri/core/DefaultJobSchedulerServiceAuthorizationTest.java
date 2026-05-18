package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobSchedulerServiceAuthorizationTest {

  private static final UUID JOB_ID = new UUID(0L, 77L);
  private static final String OWNER = "alice";
  private static final String CALLER = "bob";

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
  @Mock private JobInvocationResolver jobInvocationResolver;
  @Mock private DefaultJobCreationService jobCreationService;
  @Mock private JobAuthorizationPolicy authorizationPolicy;

  private DefaultJobSchedulerService service;

  public static void noopTask() {}

  // ---- cancelJob ----

  private static JobEntity ownerJob() {
    JobEntity e = new JobEntity();
    e.setId(JOB_ID);
    e.setJobType(JobExecutionType.SINGLE);
    e.setStatus(JobStatus.PENDING);
    e.setPriority(JobPriority.NORMAL);
    e.setCallerPrincipal(OWNER);
    return e;
  }

  @BeforeEach
  void setUp() {
    CallerPrincipalProvider callerProvider =
        new CallerPrincipalProvider(null) {
          @Override
          public Optional<String> currentPrincipal() {
            return Optional.of(CALLER);
          }
        };

    service =
        new DefaultJobSchedulerService(
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
            jobInvocationResolver,
            jobCreationService,
            callerProvider,
            authorizationPolicy,
            null,
            null);
  }

  @Test
  void cancelJob_checksAuthorizationWithOwnerAndCurrentPrincipal() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), any()))
        .thenReturn(true);

    service.cancelJob(JOB_ID);

    verify(authorizationPolicy).checkCancel(eq(JOB_ID), eq(OWNER), eq(CALLER));
  }

  // ---- pauseJob ----

  @Test
  void cancelJob_denial_throwsAndSkipsCas() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    doThrow(new JobAuthorizationException(JOB_ID, "cancel", CALLER, "denied"))
        .when(authorizationPolicy)
        .checkCancel(any(), anyString(), anyString());

    assertThrows(JobAuthorizationException.class, () -> service.cancelJob(JOB_ID));
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(), any(), any(), any());
  }

  @Test
  void cancelJob_notFoundEntity_checksWithNullOwner() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.empty());

    service.cancelJob(JOB_ID);

    // checkCancel called with null ownerPrincipal because entity not found
    verify(authorizationPolicy).checkCancel(eq(JOB_ID), eq(null), eq(CALLER));
  }

  // ---- resumeJob ----

  @Test
  void pauseJob_checksAuthorizationBeforeTransition() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    when(jobPauseStore.transitionToPaused(JOB_ID, JobStatus.PENDING)).thenReturn(true);

    service.pauseJob(JOB_ID);

    verify(authorizationPolicy).checkPause(eq(JOB_ID), eq(OWNER), eq(CALLER));
  }

  @Test
  void pauseJob_denial_throwsAndSkipsTransition() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    doThrow(new JobAuthorizationException(JOB_ID, "pause", CALLER, "denied"))
        .when(authorizationPolicy)
        .checkPause(any(), anyString(), anyString());

    assertThrows(JobAuthorizationException.class, () -> service.pauseJob(JOB_ID));
    verify(jobPauseStore, never()).transitionToPaused(any(), any());
  }

  // ---- retryJob ----

  @Test
  void resumeJob_checksAuthorizationBeforeTransition() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    when(jobPauseStore.transitionFromPausedAtomic(JOB_ID)).thenReturn(JobStatus.PENDING);

    service.resumeJob(JOB_ID);

    verify(authorizationPolicy).checkResume(eq(JOB_ID), eq(OWNER), eq(CALLER));
  }

  @Test
  void resumeJob_denial_throwsAndSkipsTransition() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    doThrow(new JobAuthorizationException(JOB_ID, "resume", CALLER, "denied"))
        .when(authorizationPolicy)
        .checkResume(any(), anyString(), anyString());

    assertThrows(JobAuthorizationException.class, () -> service.resumeJob(JOB_ID));
    verify(jobPauseStore, never()).transitionFromPausedAtomic(any());
  }

  // ---- replace ----

  @Test
  void retryJob_checksAuthorizationWithOwnerAndCurrentPrincipal() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    when(jobRetryStore.resetFailedToPending(JOB_ID)).thenReturn(true);

    service.retryJob(JOB_ID);

    verify(authorizationPolicy).checkRetry(eq(JOB_ID), eq(OWNER), eq(CALLER));
  }

  // ---- null policy (no-arg constructor) ----

  @Test
  void retryJob_denial_throwsAndSkipsCas() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    doThrow(new JobAuthorizationException(JOB_ID, "retry", CALLER, "denied"))
        .when(authorizationPolicy)
        .checkRetry(any(), anyString(), anyString());

    assertThrows(JobAuthorizationException.class, () -> service.retryJob(JOB_ID));
    verify(jobRetryStore, never()).resetFailedToPending(any());
  }

  // ---- cancelRecurringJobsByTag / cancelRecurringJobByBusinessKey ----

  @Test
  void replace_checksAuthorizationForOldJobCancellation() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    doThrow(new JobAuthorizationException(JOB_ID, "replace", CALLER, "denied"))
        .when(authorizationPolicy)
        .checkCancel(any(), anyString(), anyString());

    assertThrows(
        JobAuthorizationException.class,
        () ->
            service.replace(
                JOB_ID,
                Duration.ZERO,
                DefaultJobSchedulerServiceAuthorizationTest::noopTask,
                null));
  }

  @Test
  void replace_cancelsWaitingJobBeforeRecordingReplacement() {
    JobEntity waiting = ownerJob();
    waiting.setStatus(JobStatus.WAITING);
    UUID replacementId = new UUID(0L, 88L);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(waiting));
    when(jobCreationService.submit(any(DefaultJobBuilder.class))).thenReturn(() -> replacementId);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), any(JobStatus.class), eq(JobStatus.CANCELED), eq(null)))
        .thenAnswer(inv -> inv.getArgument(1) == JobStatus.WAITING);
    when(jobCrudStore.save(any(JobEntity.class))).thenReturn(waiting);

    service.replace(
        JOB_ID, Duration.ZERO, DefaultJobSchedulerServiceAuthorizationTest::noopTask, null);

    InOrder order = inOrder(jobCreationService, jobBatchStatusStore);
    order.verify(jobCreationService).submit(any(DefaultJobBuilder.class));
    order
        .verify(jobBatchStatusStore)
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), eq(null));
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.CANCELED), eq(null));
    verify(jobBatchStatusStore, times(4))
        .compareAndSwapStatus(eq(JOB_ID), any(), eq(JobStatus.CANCELED), eq(null));
  }

  @Test
  void replace_returnsExistingReplacementWhenOldJobAlreadySuperseded() {
    UUID replacementId = new UUID(0L, 88L);
    JobEntity oldJob = ownerJob();
    oldJob.setSupersededBy(replacementId);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(oldJob));

    assertEquals(
        replacementId,
        service
            .replace(
                JOB_ID, Duration.ZERO, DefaultJobSchedulerServiceAuthorizationTest::noopTask, null)
            .id());

    verify(jobCreationService, never()).submit(any(DefaultJobBuilder.class));
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(), any(), any(), any());
    verify(jobCrudStore, never()).save(any(JobEntity.class));
  }

  @Test
  void cancelJob_mainConstructorWithNullPolicy_skipsCheckAndProceedsNormally() {
    DefaultJobSchedulerService nullPolicyService =
        new DefaultJobSchedulerService(
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
            jobInvocationResolver,
            jobCreationService,
            null,
            null,
            null,
            null);

    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), any()))
        .thenReturn(true);

    nullPolicyService.cancelJob(JOB_ID);

    verify(jobBatchStatusStore)
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), any());
  }

  @Test
  void cancelRecurringJobsByTag_doesNotCheckAuthorization() {
    when(jobBatchStatusStore.cancelRecurringJobsByTag("tag")).thenReturn(2);
    service.cancelRecurringJobsByTag("tag");
    verify(authorizationPolicy, never()).checkCancel(any(), any(), any());
  }

  @Test
  void cancelRecurringJobByBusinessKey_doesNotCheckAuthorization() {
    when(jobBatchStatusStore.cancelRecurringJobByBusinessKey("key")).thenReturn(1);
    service.cancelRecurringJobByBusinessKey("key");
    verify(authorizationPolicy, never()).checkCancel(any(), any(), any());
  }
}
