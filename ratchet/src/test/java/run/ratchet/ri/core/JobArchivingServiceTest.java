package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.lang.reflect.Modifier;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.LockStore;

@ExtendWith(MockitoExtension.class)
class JobArchivingServiceTest {

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

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
    entity.setId(new UUID(0L, id));
    return entity;
  }

  @BeforeEach
  void setUp() {
    service =
        new JobArchivingService(
            jobBulkStore, archiveStore, singletonLeaseService, executorProvider, FIXED_CLOCK);

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

    Future<?> result = service.triggerArchiving();

    Assertions.assertTrue(result.isDone());
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
    when(jobBulkStore.deleteJobsByIds(List.of(new UUID(0L, 1L), new UUID(0L, 2L)))).thenReturn(2);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    verify(archiveStore).findJobsForArchiving(any(), eq(batchSize));
    verify(archiveStore).archiveJobsBatch(eq(batch), eq("retention_policy"), eq("system"));
    verify(jobBulkStore).deleteJobsByIds(List.of(new UUID(0L, 1L), new UUID(0L, 2L)));
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

    verify(archiveStore, times(1)).findJobsForArchiving(any(), eq(batchSize));
  }

  @Test
  void run_queriesAgainAfterFullBatchUntilNoMoreJobs() {
    int batchSize = 2;
    service.init(true, 7, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> fullBatch = List.of(jobEntity(20L), jobEntity(21L));
    when(archiveStore.countJobsForArchiving(any())).thenReturn(2L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize)))
        .thenReturn(fullBatch)
        .thenReturn(List.of());
    when(archiveStore.archiveJobsBatch(eq(fullBatch), anyString(), anyString())).thenReturn(2);
    when(jobBulkStore.deleteJobsByIds(any())).thenReturn(2);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    verify(archiveStore, times(2)).findJobsForArchiving(any(), eq(batchSize));
    verify(archiveStore, times(1)).archiveJobsBatch(eq(fullBatch), anyString(), anyString());
  }

  @Test
  void run_deletesOnlyArchivedRowsWhenStoreArchivesPartialBatch() {
    int batchSize = 50;
    service.init(true, 7, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> batch = List.of(jobEntity(30L), jobEntity(31L), jobEntity(32L));
    when(archiveStore.countJobsForArchiving(any())).thenReturn(3L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize))).thenReturn(batch);
    when(archiveStore.archiveJobsBatch(eq(batch), anyString(), anyString())).thenReturn(2);
    when(jobBulkStore.deleteJobsByIds(any())).thenReturn(2);
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    verify(jobBulkStore).deleteJobsByIds(List.of(new UUID(0L, 30L), new UUID(0L, 31L)));
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
  void schedulerConfigurationReadByWorkerThreads_isVolatile() throws Exception {
    Assertions.assertTrue(
        Modifier.isVolatile(JobArchivingService.class.getDeclaredField("enabled").getModifiers()));
    Assertions.assertTrue(
        Modifier.isVolatile(JobArchivingService.class.getDeclaredField("cron").getModifiers()));
    Assertions.assertTrue(
        Modifier.isVolatile(JobArchivingService.class.getDeclaredField("zone").getModifiers()));
    Assertions.assertTrue(
        Modifier.isVolatile(
            JobArchivingService.class.getDeclaredField("retentionPeriod").getModifiers()));
    Assertions.assertTrue(
        Modifier.isVolatile(
            JobArchivingService.class.getDeclaredField("batchSize").getModifiers()));
  }

  @Test
  void run_doesNotDeleteRows_whenBatchArchiveFails() {
    int batchSize = 2;
    service.init(true, 7, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());

    List<JobEntity> batch = List.of(jobEntity(1L), jobEntity(2L));
    when(archiveStore.countJobsForArchiving(any())).thenReturn(2L);
    when(archiveStore.findJobsForArchiving(any(), eq(batchSize)))
        .thenReturn(batch)
        .thenReturn(List.of());
    when(archiveStore.archiveJobsBatch(eq(batch), anyString(), anyString()))
        .thenThrow(new IllegalStateException("archive failed"));
    when(archiveStore.purgeArchivedJobs(any())).thenReturn(0);

    service.run();

    verify(jobBulkStore, never()).deleteJobsByIds(any());
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
    Instant expected = FIXED_NOW.minus(Duration.ofDays((long) retentionDays * 3));

    Assertions.assertEquals(expected, cutoff);
  }

  @Test
  void runUsesFixedClockForArchiveCutoff() {
    int retentionDays = 7;
    int batchSize = 50;
    service.init(true, retentionDays, batchSize, parsedCron());

    when(singletonLeaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(acquiredLease());
    when(archiveStore.countJobsForArchiving(any())).thenReturn(0L);

    service.run();

    verify(archiveStore).countJobsForArchiving(FIXED_NOW.minus(Duration.ofDays(retentionDays)));
  }

  @Test
  void initSchedulesNextExecutionFromFixedClock() {
    Cron cron = parsedCron();

    service.init(true, 7, 100, cron);

    Instant next =
        ExecutionTime.forCron(cron)
            .nextExecution(FIXED_NOW.atZone(ZoneOffset.UTC))
            .map(ZonedDateTime::toInstant)
            .orElseThrow();
    verify(scheduledExecutor)
        .schedule(
            any(Runnable.class),
            eq(Duration.between(FIXED_NOW, next).toMillis()),
            eq(TimeUnit.MILLISECONDS));
  }

  @Test
  void stopPreventsRunFromSchedulingNextExecution() {
    service.init(true, 7, 100, parsedCron());
    service.stop();

    when(singletonLeaseService.tryAcquire(eq("jobArchiver"), any(Duration.class)))
        .thenReturn(Optional.empty());

    service.run();

    verify(scheduledExecutor, times(1))
        .schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class));
  }

  @Test
  void stopDoesNotAdvertiseTransactionalRollbackSemantics() throws Exception {
    Transactional tx =
        JobArchivingService.class.getMethod("stop").getAnnotation(Transactional.class);

    Assertions.assertNotNull(tx);
    Assertions.assertEquals(TxType.NOT_SUPPORTED, tx.value());
  }

  private Optional<SingletonLease> acquiredLease() {
    return Optional.of(new SingletonLease(lockStore, "jobArchiver", "node-1"));
  }
}
