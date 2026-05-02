package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
            authorizationPolicy);
  }

  // ---- cancelJob ----

  @Test
  void cancelJob_checksAuthorizationWithOwnerAndCurrentPrincipal() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), any()))
        .thenReturn(true);

    service.cancelJob(JOB_ID);

    verify(authorizationPolicy).checkCancel(eq(JOB_ID), eq(OWNER), eq(CALLER));
  }

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

  // ---- pauseJob ----

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

  // ---- resumeJob ----

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

  // ---- retryJob ----

  @Test
  void retryJob_checksAuthorizationWithOwnerAndCurrentPrincipal() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    when(jobRetryStore.resetFailedToPending(JOB_ID)).thenReturn(true);

    service.retryJob(JOB_ID);

    verify(authorizationPolicy).checkRetry(eq(JOB_ID), eq(OWNER), eq(CALLER));
  }

  @Test
  void retryJob_denial_throwsAndSkipsCas() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(ownerJob()));
    doThrow(new JobAuthorizationException(JOB_ID, "retry", CALLER, "denied"))
        .when(authorizationPolicy)
        .checkRetry(any(), anyString(), anyString());

    assertThrows(JobAuthorizationException.class, () -> service.retryJob(JOB_ID));
    verify(jobRetryStore, never()).resetFailedToPending(any());
  }

  // ---- replace ----

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
                java.time.Duration.ZERO,
                DefaultJobSchedulerServiceAuthorizationTest::noopTask,
                null));
  }

  // ---- null policy (no-arg constructor) ----

  @Test
  void cancelJob_nullPolicy_skipsCheckAndProceedsNormally() {
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
            recurringScheduler);

    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), any()))
        .thenReturn(true);

    nullPolicyService.cancelJob(JOB_ID);

    verify(jobBatchStatusStore)
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.PENDING), eq(JobStatus.CANCELED), any());
  }

  public static void noopTask() {}

  private static JobEntity ownerJob() {
    JobEntity e = new JobEntity();
    e.setId(JOB_ID);
    e.setJobType(JobExecutionType.SINGLE);
    e.setStatus(JobStatus.PENDING);
    e.setPriority(JobPriority.NORMAL);
    e.setCallerPrincipal(OWNER);
    return e;
  }
}
