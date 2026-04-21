package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.LockStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobArchivingServiceTest {

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  // Fires once a day — a valid Quartz cron used to satisfy init() without real scheduling
  private static final String DAILY_CRON = "0 0 2 * * ?";

  @Mock private JobBulkStore jobBulkStore;
  @Mock private ArchiveStore archiveStore;
  @Mock private SingletonLeaseService singletonLeaseService;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ExecutorService jobExecutor;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private LockStore lockStore;

  @SuppressWarnings("rawtypes")
  @Mock
  private ScheduledFuture scheduledFuture;

  private JobArchivingService service;

  private static Cron parsedCron() {
    return CRON_PARSER.parse(DAILY_CRON);
  }

  private static JobEntity jobEntity(long id) {
    JobEntity entity = new JobEntity();
    entity.setId(id);
    return entity;
  }

  @BeforeEach
  void setUp() {
    service =
        new JobArchivingService(
            jobBulkStore, archiveStore, singletonLeaseService, executorProvider);

    lenient().when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    lenient()
        .when(scheduledExecutor.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
        .thenAnswer(inv -> scheduledFuture);
  }

  @Test
  void run_skipsAllStoreInteractions_whenDisabled() {
    service.init(false, 7, 100, parsedCron());

    service.run();

    verify(singletonLeaseService, never()).tryAcquire(anyString(), any(Duration.class));
    verify(archiveStore, never()).countJobsForArchiving(any());
    verify(archiveStore, never()).findJobsForArchiving(any(), anyInt());
    verify(archiveStore, never()).archiveJobsBatch(any(), anyString(), anyString());
    verify(jobBulkStore, never()).deleteJobsByIds(any());
  }

  @Test
  void run_skipsArchiving_whenLockCannotBeAcquired() {
    service.init(true, 7, 100, parsedCron());

    when(singletonLeaseService.tryAcquire(eq("jobArchiver"), any(Duration.class)))
        .thenReturn(Optional.empty());

    service.run();

    verify(archiveStore, never()).countJobsForArchiving(any());
    verify(archiveStore, never()).findJobsForArchiving(any(), anyInt());
    verify(archiveStore, never()).archiveJobsBatch(any(), anyString(), anyString());
    verify(jobBulkStore, never()).deleteJobsByIds(any());
  }

  @Test
  void run_acquiresLockWithTwoHourTtl_whenEnabled() {
    service.init(true, 7, 100, parsedCron());

    when(singletonLeaseService.tryAcquire(eq("jobArchiver"), any(Duration.class)))
        .thenReturn(Optional.empty());

    service.run();

    verify(singletonLeaseService).tryAcquire(eq("jobArchiver"), eq(Duration.ofHours(2)));
  }

  @Test
  void triggerArchiving_doesNotSubmitWork_whenDisabled() {
    service.init(false, 7, 100, parsedCron());

    service.triggerArchiving();

    verify(executorProvider, never()).getJobExecutor();
  }

  @Test
  void triggerArchiving_submitsToJobExecutor_whenEnabled() {
    service.init(true, 7, 100, parsedCron());

    when(executorProvider.getJobExecutor()).thenReturn(jobExecutor);

    service.triggerArchiving();

    verify(executorProvider).getJobExecutor();
    verify(jobExecutor).submit(any(Runnable.class));
  }

  @Test
  void run_doesNotCallFindOrArchive_whenNoJobsEligible() {
    service.init(true, 7, 100, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());
    when(archiveStore.countJobsForArchiving(any())).thenReturn(0L);

    service.run();

    verify(archiveStore, never()).findJobsForArchiving(any(), anyInt());
    verify(archiveStore, never()).archiveJobsBatch(any(), anyString(), anyString());
    verify(jobBulkStore, never()).deleteJobsByIds(any());
  }

  @Test
  void run_archivesAndDeletesJobs_happyPathSingleBatch() {
    int batchSize = 50;
    service.init(true, 7, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> batch = List.of(jobEntity(1L), jobEntity(2L));

    when(archiveStore.countJobsForArchiving(any())).thenReturn(2L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize))).thenReturn(batch);
    when(archiveStore.archiveJobsBatch(eq(batch), eq("retention_policy"), eq("system")))
        .thenReturn(2);
    when(jobBulkStore.deleteJobsByIds(List.of(1L, 2L))).thenReturn(2);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    verify(archiveStore).findJobsForArchiving(any(), eq(batchSize));
    verify(archiveStore).archiveJobsBatch(eq(batch), eq("retention_policy"), eq("system"));
    verify(jobBulkStore).deleteJobsByIds(List.of(1L, 2L));
    verify(archiveStore).purgeArchivedJobs(any());
  }

  @Test
  void run_stopsAfterIncompleteBatch_withoutQueryingAgain() {
    int batchSize = 50;
    service.init(true, 7, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> smallBatch = List.of(jobEntity(10L), jobEntity(11L), jobEntity(12L));

    when(archiveStore.countJobsForArchiving(any())).thenReturn(3L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize))).thenReturn(smallBatch);
    when(archiveStore.archiveJobsBatch(eq(smallBatch), anyString(), anyString())).thenReturn(3);
    when(jobBulkStore.deleteJobsByIds(any())).thenReturn(3);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    verify(archiveStore).findJobsForArchiving(any(), eq(batchSize));
  }

  @Test
  void run_purgesOldArchivedJobs_afterArchivingCompletes() {
    int batchSize = 50;
    service.init(true, 7, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> batch = List.of(jobEntity(1L));
    when(archiveStore.countJobsForArchiving(any())).thenReturn(1L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize))).thenReturn(batch);
    when(archiveStore.archiveJobsBatch(eq(batch), anyString(), anyString())).thenReturn(1);
    when(jobBulkStore.deleteJobsByIds(any())).thenReturn(1);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(5);

    service.run();

    verify(archiveStore).purgeArchivedJobs(any());
  }

  @Test
  void performArchiveCleanup_usesCutoffOfThreeTimesRetentionPeriod() {
    int retentionDays = 10;
    int batchSize = 50;
    service.init(true, retentionDays, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> batch = List.of(jobEntity(99L));
    when(archiveStore.countJobsForArchiving(any())).thenReturn(1L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize))).thenReturn(batch);
    when(archiveStore.archiveJobsBatch(eq(batch), anyString(), anyString())).thenReturn(1);
    when(jobBulkStore.deleteJobsByIds(any())).thenReturn(1);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(archiveStore).purgeArchivedJobs(cutoffCaptor.capture());

    Instant cutoff = cutoffCaptor.getValue();
    Instant expectedMin = Instant.now().minus(Duration.ofDays((long) retentionDays * 3 + 1));
    Instant expectedMax = Instant.now().minus(Duration.ofDays((long) retentionDays * 3 - 1));

    Assertions.assertTrue(cutoff.isAfter(expectedMin) && cutoff.isBefore(expectedMax));
  }

  private Optional<SingletonLease> acquiredLease() {
    return Optional.of(new SingletonLease(lockStore, "jobArchiver", "node-1"));
  }
}
