package run.ratchet.ri.core;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import jakarta.enterprise.inject.Instance;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobWakeupServiceTest {

  @Mock private ClusterCoordinator clusterCoordinator;
  @Mock private Instance<PollerScheduler> pollerSchedulerInstance;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private TransactionSynchronizationRegistry txRegistry;
  @Mock private MetricsCollector metricsCollector;

  private JobWakeupService wakeupService;

  @BeforeEach
  void setUp() {
    wakeupService =
        new JobWakeupService(clusterCoordinator, pollerSchedulerInstance, metricsCollector);
  }

  @Test
  void notify_wakesLocalPollerAndClusterImmediatelyWithoutTransaction() {
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    wakeupService.notify(JobPriority.NORMAL, true);

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.NORMAL);
  }

  @Test
  void notify_registersAfterCommitWhenTransactionActive() throws Exception {
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
        .registerInterposedSynchronization(org.mockito.ArgumentMatchers.any());
    injectTxRegistry(txRegistry);

    wakeupService.notify(JobPriority.CRITICAL, true);

    verify(pollerScheduler, never()).wakeup();
    verify(clusterCoordinator, never()).notifyNewWork(JobPriority.CRITICAL);

    synchronization.get().afterCompletion(Status.STATUS_COMMITTED);

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.CRITICAL);
  }

  @Test
  void notify_doesNothingWhenImmediateFlagIsFalse() {
    wakeupService.notify(JobPriority.NORMAL, false);

    verify(pollerScheduler, never()).wakeup();
    verify(clusterCoordinator, never()).notifyNewWork(JobPriority.NORMAL);
  }

  @Test
  void notify_fallsBackToImmediateWhenAfterCommitRegistrationFails() throws Exception {
    when(pollerSchedulerInstance.isResolvable()).thenReturn(true);
    when(pollerSchedulerInstance.get()).thenReturn(pollerScheduler);

    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doThrow(new IllegalStateException("boom"))
        .when(txRegistry)
        .registerInterposedSynchronization(org.mockito.ArgumentMatchers.any());
    injectTxRegistry(txRegistry);

    wakeupService.notify(JobPriority.HIGH, true);

    verify(metricsCollector).localWakeup("job_submit");
    verify(pollerScheduler).wakeup();
    verify(clusterCoordinator).notifyNewWork(JobPriority.HIGH);
  }

  private void injectTxRegistry(TransactionSynchronizationRegistry registry) throws Exception {
    Field field = JobWakeupService.class.getDeclaredField("txRegistry");
    field.setAccessible(true);
    field.set(wakeupService, registry);
  }
}
