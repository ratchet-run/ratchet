package run.ratchet.ri.core.internal;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class JobWakeupServiceTest {

  private static final String NODE_ID = "node-test";
  private static final NodeIdentity NODE_IDENTITY = new NodeIdentity(NODE_ID);

  @Mock private ClusterCoordinator clusterCoordinator;
  @Mock private Instance<PollerScheduler> pollerSchedulerInstance;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private TransactionSynchronizationRegistry txRegistry;
  @Mock private MetricsCollector metricsCollector;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private JobWakeupService wakeupService;

  @BeforeEach
  void setUp() {
    wakeupService =
        new JobWakeupService(
            clusterCoordinator, pollerSchedulerInstance, metricsCollector, nodeIdentityProvider);
  }

  @Test
  void notify_wakesLocalPollerAndClusterImmediatelyWithoutTransaction() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    wakeupService.notify(JobPriority.NORMAL, true);

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.NORMAL, NODE_IDENTITY);
  }

  @Test
  void notify_registersAfterCommitWhenTransactionActive() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    AtomicReference<Synchronization> synchronization = new AtomicReference<>();
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doAnswer(
            invocation -> {
              synchronization.set(invocation.getArgument(0));
              return null;
            })
        .when(txRegistry)
        .registerInterposedSynchronization(ArgumentMatchers.any());
    wakeupService =
        new JobWakeupService(
            clusterCoordinator,
            pollerSchedulerInstance,
            metricsCollector,
            nodeIdentityProvider,
            txRegistry);

    wakeupService.notify(JobPriority.CRITICAL, true);

    verify(pollerScheduler, never()).wakeup();
    verify(clusterCoordinator, never()).notifyNewWork(eq(JobPriority.CRITICAL), any());

    synchronization.get().afterCompletion(Status.STATUS_COMMITTED);

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.CRITICAL, NODE_IDENTITY);
  }

  @Test
  void notify_doesNothingWhenImmediateFlagIsFalse() {
    wakeupService.notify(JobPriority.NORMAL, false);

    verify(pollerScheduler, never()).wakeup();
    verify(clusterCoordinator, never()).notifyNewWork(eq(JobPriority.NORMAL), any());
  }

  @Test
  void notify_fallsBackToImmediateWhenAfterCommitRegistrationFails() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doThrow(new IllegalStateException("boom"))
        .when(txRegistry)
        .registerInterposedSynchronization(ArgumentMatchers.any());
    wakeupService =
        new JobWakeupService(
            clusterCoordinator,
            pollerSchedulerInstance,
            metricsCollector,
            nodeIdentityProvider,
            txRegistry);

    wakeupService.notify(JobPriority.HIGH, true);

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.HIGH, NODE_IDENTITY);
  }

  @Test
  void notifyIfNeeded_publishesCriticalPriorityEvenWhenDelayedBatchChild() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    wakeupService.notifyIfNeeded(
        JobExecutionType.BATCH_CHILD, JobPriority.CRITICAL, Duration.ofMinutes(5));

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.CRITICAL, NODE_IDENTITY);
  }

  @Test
  void notifyIfNeeded_publishesSingleJobsWithZeroOrNullDelay() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    wakeupService.notifyIfNeeded(JobExecutionType.SINGLE, JobPriority.NORMAL, Duration.ZERO);
    wakeupService.notifyIfNeeded(JobExecutionType.SINGLE, JobPriority.LOW, null);

    verify(metricsCollector, times(2)).localWakeup("job_submit");
    verify(pollerScheduler, times(2)).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.NORMAL, NODE_IDENTITY);
    verify(clusterCoordinator).notifyNewWork(JobPriority.LOW, NODE_IDENTITY);
  }

  @Test
  void notifyIfNeeded_publishesBatchParentRegardlessOfDelay() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    wakeupService.notifyIfNeeded(
        JobExecutionType.BATCH_PARENT, JobPriority.LOWEST, Duration.ofHours(1));

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.LOWEST, NODE_IDENTITY);
  }

  @Test
  void notifyIfNeeded_skipsDelayedSingleAndBatchChildWhenNotCritical() {
    wakeupService.notifyIfNeeded(JobExecutionType.SINGLE, JobPriority.NORMAL, Duration.ofMillis(1));
    wakeupService.notifyIfNeeded(JobExecutionType.BATCH_CHILD, JobPriority.HIGH, Duration.ZERO);

    verifyNoInteractions(
        clusterCoordinator, pollerSchedulerInstance, pollerScheduler, metricsCollector);
  }

  @Test
  void nodeIdentity_isResolvedOnceAcrossManyPublishes() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    for (int i = 0; i < 5; i++) {
      wakeupService.notify(JobPriority.NORMAL, true);
    }

    // NodeIdentityProvider is stable per JVM lifetime by SPI contract; the cache must collapse
    // the 5 provider lookups + 5 NodeIdentity validations into one.
    verify(nodeIdentityProvider, times(1)).getNodeId();
    verify(clusterCoordinator, times(5)).notifyNewWork(JobPriority.NORMAL, NODE_IDENTITY);
  }
}
