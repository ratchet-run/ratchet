package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code JobCrudStore}. */
public abstract class AbstractJobCrudStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupCrudFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindById_roundTripsPersistedJob() {
    var saved = persist(newPendingJob());

    var reloaded = store().findById(saved.getId());

    assertTrue(reloaded.isPresent(), "Persisted job should be reloadable by ID");
    assertEquals(saved.getId(), reloaded.get().getId());
  }

  @Test
  void saveAndFindById_roundTripsCallerPrincipalField() {
    JobEntity job = newPendingJob();
    job.setCallerPrincipal("alice");
    JobEntity saved = persist(job);

    JobEntity reloaded = store().findById(saved.getId()).orElseThrow();

    assertEquals(
        "alice",
        reloaded.getCallerPrincipal(),
        "Caller principal must round-trip through save/findById");
  }

  @Test
  void findByIdLatest_roundTripsPersistedJob() {
    var saved = persist(newPendingJob());

    var reloaded = store().findByIdLatest(saved.getId());

    assertTrue(reloaded.isPresent(), "Persisted job should be reloadable by latest read");
    assertEquals(saved.getId(), reloaded.get().getId());
  }

  @Test
  void findByIds_returnsEveryRequestedRow() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());

    var jobs = store().findByIds(List.of(first.getId(), second.getId()));

    assertEquals(2, jobs.size(), "findByIds should return both persisted jobs");
  }

  @Test
  void findByIdempotencyKey_returnsMatchingJob() {
    var saved = persist(newPendingJob());

    var reloaded = store().findByIdempotencyKey(saved.getIdempotencyKey());

    assertTrue(reloaded.isPresent(), "Persisted job should be reloadable by idempotency key");
    assertEquals(saved.getId(), reloaded.get().getId());
  }

  @Test
  void save_concurrentMutation_oneThreadObservesStaleWrite() {
    JobEntity initial = persist(newPendingJob());
    UUID id = initial.getId();

    // Pre-load both snapshots BEFORE the latch release. Both hold the same version.
    JobEntity snapshotA = store().findById(id).orElseThrow();
    JobEntity snapshotB = store().findById(id).orElseThrow();

    List<Throwable> failures =
        ConcurrentTestRunner.runAll(
            Duration.ofSeconds(10),
            () -> {
              snapshotA.setStatus(JobStatus.RUNNING);
              store().save(snapshotA);
            },
            () -> {
              snapshotB.setStatus(JobStatus.CANCELED);
              store().save(snapshotB);
            });

    long staleWriteCount =
        failures.stream().filter(t -> t != null && isStaleWriteException(t)).count();
    long otherFailureCount =
        failures.stream().filter(t -> t != null && !isStaleWriteException(t)).count();

    assertEquals(
        0L,
        otherFailureCount,
        "no thread should fail with a non-stale-write exception; got " + failures);
    assertEquals(
        1L,
        staleWriteCount,
        "exactly one thread must observe a stale-write failure; got " + failures);
  }

  @Test
  void save_staleLiveSnapshotAfterTerminalization_reportsStaleWrite() {
    JobEntity initial = persist(newPendingJob());
    UUID id = initial.getId();

    JobEntity staleLiveSnapshot = store().findById(id).orElseThrow();
    JobEntity terminalizingSnapshot = store().findById(id).orElseThrow();

    terminalizingSnapshot.setStatus(JobStatus.CANCELED);
    store().save(terminalizingSnapshot);

    staleLiveSnapshot.setStatus(JobStatus.RUNNING);
    RuntimeException failure =
        assertThrows(RuntimeException.class, () -> store().save(staleLiveSnapshot));

    assertTrue(
        isStaleWriteException(failure),
        "a pre-terminal live snapshot must fail as a stale write after terminalization; got "
            + failure);
  }

  @Test
  void findById_unknownId_returnsEmpty() {
    var result = store().findById(new UUID(0L, Long.MAX_VALUE));

    assertTrue(result.isEmpty(), "findById with unknown ID should return empty");
  }

  @Test
  void delete_removesJob() {
    var saved = persist(newPendingJob());
    UUID id = saved.getId();

    store().delete(id);

    assertTrue(store().findById(id).isEmpty(), "Deleted job should not be found");
  }

  @Test
  void findActiveByBusinessKey_returnsMatchingJob() {
    var job = newPendingJob();
    job.setBusinessKey("bk-test-" + job.getIdempotencyKey());
    var saved = persist(job);

    var result = store().findActiveByBusinessKey(saved.getBusinessKey());

    assertTrue(result.isPresent(), "findActiveByBusinessKey should return the matching job");
    assertEquals(saved.getId(), result.get().getId());
  }

  @Test
  void findActiveByBusinessKey_ignoresTerminalJobs() {
    var job = newPendingJob();
    job.setBusinessKey("bk-terminal-" + job.getIdempotencyKey());
    var saved = persist(job);
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(saved.getId(), null, null, Instant.now(), Instant.now(), 0L, 0L);

    // Guard: verify the CAS actually landed before testing the query filter
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.SUCCEEDED, reloaded.getStatus(), "Job should be SUCCEEDED");

    var result = store().findActiveByBusinessKey(saved.getBusinessKey());

    assertFalse(
        result.isPresent(), "findActiveByBusinessKey should not return terminal (SUCCEEDED) jobs");
  }

  @Test
  void findDependants_returnsDependentJobs() {
    var parent = persist(newPendingJob());

    var child1 = newPendingJob();
    child1.setDependsOn(parent.getId());
    persist(child1);

    var child2 = newPendingJob();
    child2.setDependsOn(parent.getId());
    persist(child2);

    var dependants = store().findDependants(parent.getId());

    assertEquals(2, dependants.size(), "findDependants should return both dependent jobs");
  }

  @Test
  void findDependants_returnsRequestedPage() {
    var parent = persist(newPendingJob());

    var child1 = newPendingJob();
    child1.setDependsOn(parent.getId());
    var saved1 = persist(child1);

    var child2 = newPendingJob();
    child2.setDependsOn(parent.getId());
    var saved2 = persist(child2);

    var page0 = store().findDependants(parent.getId(), 1, 0);
    var page1 = store().findDependants(parent.getId(), 1, 1);

    assertEquals(1, page0.size(), "first dependant page should honor limit");
    assertEquals(1, page1.size(), "second dependant page should honor offset");
    assertTrue(
        List.of(saved1.getId(), saved2.getId()).contains(page0.get(0).getId()),
        "first page should contain one saved child");
    assertTrue(
        List.of(saved1.getId(), saved2.getId()).contains(page1.get(0).getId()),
        "second page should contain one saved child");
  }

  @Test
  void countPendingJobs_returnsAccurateCount() {
    persist(newPendingJob());
    persist(newPendingJob());
    persist(newPendingJob());

    var running = persist(newPendingJob());
    store().compareAndSwapStatus(running.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    long count = store().countPendingJobs();

    assertEquals(3L, count, "countPendingJobs should count only PENDING jobs");
  }

  @Test
  void countJobsByStatuses_returnsGroupedStatusCounts() {
    persist(newPendingJob());

    var running = persist(newPendingJob());
    store().compareAndSwapStatus(running.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    var succeeded = persist(newPendingJob());
    store().compareAndSwapStatus(succeeded.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(succeeded.getId(), null, null, Instant.now(), Instant.now(), 0L, 0L);

    Map<JobStatus, Long> counts = store().countJobsByStatuses();

    assertEquals(1L, counts.get(JobStatus.PENDING));
    assertEquals(1L, counts.get(JobStatus.RUNNING));
    assertEquals(1L, counts.get(JobStatus.SUCCEEDED));
  }

  @Test
  void countPendingJobsByPriorities_returnsGroupedPendingCounts() {
    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    persist(high);

    JobEntity critical = newPendingJob();
    critical.setPriority(JobPriority.CRITICAL);
    persist(critical);

    JobEntity running = newPendingJob();
    running.setPriority(JobPriority.HIGH);
    JobEntity savedRunning = persist(running);
    store().compareAndSwapStatus(savedRunning.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Map<JobPriority, Long> counts = store().countPendingJobsByPriorities();

    assertEquals(1L, counts.get(JobPriority.HIGH));
    assertEquals(1L, counts.get(JobPriority.CRITICAL));
  }

  @Test
  void countPendingJobsByTypes_returnsGroupedPendingCounts() {
    JobEntity single = newPendingJob();
    single.setJobType(JobExecutionType.SINGLE);
    persist(single);

    JobEntity child = newPendingJob();
    child.setJobType(JobExecutionType.BATCH_CHILD);
    persist(child);

    JobEntity running = newPendingJob();
    running.setJobType(JobExecutionType.SINGLE);
    JobEntity savedRunning = persist(running);
    store().compareAndSwapStatus(savedRunning.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Map<JobExecutionType, Long> counts = store().countPendingJobsByTypes();

    assertEquals(1L, counts.get(JobExecutionType.SINGLE));
    assertEquals(1L, counts.get(JobExecutionType.BATCH_CHILD));
  }

  @Test
  void create_setsCreatedAt() {
    JobEntity job = newPendingJob();

    JobEntity created = store().create(job);

    assertNotNull(created.getCreatedAt(), "create() must populate createdAt");
  }

  @Test
  void create_duplicateId_throws() {
    JobEntity job = newPendingJob();
    store().create(job);

    assertThrows(
        RuntimeException.class,
        () -> store().create(job),
        "create() must reject a duplicate ID (insert-only semantics)");
  }

  @Test
  void save_preservesCreatedAt() {
    JobEntity job = newPendingJob();
    JobEntity created = store().create(job);
    Instant originalCreatedAt = created.getCreatedAt();
    assertNotNull(originalCreatedAt, "create() must set createdAt before the save round-trip");

    created.setStatus(JobStatus.RUNNING);
    JobEntity updated = store().save(created);

    assertEquals(
        originalCreatedAt,
        updated.getCreatedAt(),
        "save() must not overwrite the createdAt set by create()");
  }
}
