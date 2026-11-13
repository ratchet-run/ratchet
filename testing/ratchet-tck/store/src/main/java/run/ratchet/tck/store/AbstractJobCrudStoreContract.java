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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.EventOutboxStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPropertyStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code JobCrudStore}. */
public abstract class AbstractJobCrudStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupCrudFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindById_roundTripsLargeMultibytePayload() {
    String value = "é".repeat(128 * 1024);
    var job = newPendingJob();
    job.setPayload(
        new JobPayload(
            "java.lang.String",
            "valueOf",
            "(Ljava/lang/Object;)Ljava/lang/String;",
            true,
            List.of(value),
            null));
    var saved = store().save(job);
    assertEquals(job.getPayload(), store().findById(saved.getId()).orElseThrow().getPayload());
  }

  @Test
  void save_terminalFailed_roundTripsLargeMultibyteError() {
    String error = "é".repeat(128 * 1024);
    var saved = persist(newPendingJob());
    saved = store().findById(saved.getId()).orElseThrow();
    saved.setStatus(JobStatus.FAILED);
    saved.setLastError(error);
    store().save(saved);
    var found = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.FAILED, found.getStatus());
    assertEquals(error, found.getLastError());
  }

  @Test
  void saveAndFindById_roundTripsPersistedJob() {
    var saved = persist(newPendingJob());

    var reloaded = store().findById(saved.getId());

    assertTrue(reloaded.isPresent(), "Persisted job should be reloadable by ID");
    assertEquals(saved.getId(), reloaded.get().getId());
  }

  @Test
  void capabilityProbe_agreesWithJavaTypeMembership() {
    // Type membership is the single normative rule for capability advertisement: a store that
    // overrides capability() to report a capability it does not implement, or to hide one it does,
    // desynchronizes the probe from every dependency-injection consumer (the reference engine
    // resolves each capability through CDI Instance<T>). The probe must therefore agree with
    // instanceof, and a present capability must hand back a view of the requested type.
    var store = store();
    List<Class<?>> capabilities =
        List.of(
            RecurringJobStore.class,
            BatchStore.class,
            PartitionStore.class,
            WorkflowConditionStore.class,
            SignalStore.class,
            ResourcePermitStore.class,
            LockStore.class,
            ArchiveStore.class,
            JobQueryStore.class,
            JobAnalyticsStore.class,
            JobAuditStore.class,
            JobPropertyStore.class,
            JobStateStore.class,
            EventOutboxStore.class);
    for (Class<?> cap : capabilities) {
      var view = store.capability(cap);
      assertEquals(
          cap.isInstance(store),
          view.isPresent(),
          () -> "capability(" + cap.getSimpleName() + ") must agree with Java type membership");
      view.ifPresent(
          v ->
              assertTrue(
                  cap.isInstance(v),
                  () -> "capability(" + cap.getSimpleName() + ") must return a view of that type"));
    }
  }

  @Test
  void saveAndFindById_coercesNullCronAndZoneToStoredDefaults() {
    JobEntity job = newPendingJob();
    job.setCronExpr(null);
    job.setZoneId(null);

    JobEntity saved = persist(job);
    JobEntity reloaded = store().findById(saved.getId()).orElseThrow();

    assertEquals(
        "",
        reloaded.getCronExpr(),
        "null cronExpr must be coerced to an empty string on every store");
    assertEquals("UTC", reloaded.getZoneId(), "null zoneId must be coerced to UTC on every store");
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
  void idempotencyKeySurvivesDeletionAndRejectsReuse() {
    var saved = persist(newPendingJob());
    store().delete(saved.getId());
    assertEquals(
        Optional.of(saved.getId()),
        store().findOriginalJobIdByIdempotencyKey(saved.getIdempotencyKey()));
    var duplicate = newPendingJob();
    duplicate.setIdempotencyKey(saved.getIdempotencyKey());
    assertThrows(DuplicateIdempotencyKeyException.class, () -> store().create(duplicate));
    assertEquals(
        Optional.of(saved.getId()),
        store().findOriginalJobIdByIdempotencyKey(saved.getIdempotencyKey()));
  }

  @Test
  void idempotencyKeySurvivesArchiveAndPurge() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            saved.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 1000L, 0L);
    var archive = archiveStore();
    archive.archiveAndDeleteJobsBatch(
        List.of(store().findById(saved.getId()).orElseThrow()), "tck", "tck");
    archive.purgeArchivedJobs(Instant.now().plusSeconds(60));
    assertFalse(store().findById(saved.getId()).isPresent());
    assertEquals(
        Optional.of(saved.getId()),
        store().findOriginalJobIdByIdempotencyKey(saved.getIdempotencyKey()));
    var duplicate = newPendingJob();
    duplicate.setIdempotencyKey(saved.getIdempotencyKey());
    assertThrows(DuplicateIdempotencyKeyException.class, () -> store().create(duplicate));
  }

  @Test
  void save_concurrentMutation_oneThreadObservesStaleWrite() {
    // Skip cleanly if this fixture does not surface optimistic-lock failures (e.g., MongoDB
    // standalone without replica-set transactions). Without this gate the test fails with a
    // misleading "exactly one thread must observe a stale-write failure" message that looks
    // like a real concurrency bug rather than a missing fixture override.
    Assumptions.assumeTrue(
        isStaleWriteException(new ConcurrentModificationException("probe")),
        "Fixture does not surface optimistic-lock failures; skipping stale-write contract.");

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
  void findBusinessKeyHolder_returnsMatchingJob() {
    var job = newPendingJob();
    job.setBusinessKey("bk-test-" + job.getIdempotencyKey());
    var saved = persist(job);

    var result = store().findBusinessKeyHolder(saved.getBusinessKey());

    assertTrue(result.isPresent(), "findBusinessKeyHolder should return the matching job");
    assertEquals(saved.getId(), result.get().id());
  }

  @Test
  void findBusinessKeyHolder_ignoresTerminalJobs() {
    var job = newPendingJob();
    job.setBusinessKey("bk-terminal-" + job.getIdempotencyKey());
    var saved = persist(job);
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(saved.getId(), null, null, Instant.now(), Instant.now(), 0L, 0L);

    // Guard: verify the CAS actually landed before testing the query filter
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.SUCCEEDED, reloaded.getStatus(), "Job should be SUCCEEDED");

    var result = store().findBusinessKeyHolder(saved.getBusinessKey());

    assertFalse(
        result.isPresent(), "findBusinessKeyHolder should not return terminal (SUCCEEDED) jobs");
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

    var dependants = store().findDependants(parent.getId(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);

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

  @Test
  void findStatusesByIds_returnsMixedStatuses() {
    // Status reads include live and terminal jobs, including both cancel paths.
    var succeeded = persist(newPendingJob());
    assertEquals(succeeded.getId(), store().claimNextBatch(1, "status-node").get(0).getId());
    assertTrue(
        store().markJobSucceededMinimal(succeeded.getId(), Instant.now(), Instant.now(), 0L, 0L));
    var failed = persist(newPendingJob());
    var failedClaim = store().claimNextBatch(1, "status-node").get(0);
    assertEquals(failed.getId(), failedClaim.getId());
    assertTrue(
        store().markJobFailedTerminal(failed.getId(), "failed", 1, failedClaim.getClaimSeq()));
    var canceledRunning = persist(newPendingJob());
    assertEquals(canceledRunning.getId(), store().claimNextBatch(1, "status-node").get(0).getId());
    assertTrue(store().cancelRunningJob(canceledRunning.getId(), null));
    var running = persist(newPendingJob());
    assertEquals(running.getId(), store().claimNextBatch(1, "status-node").get(0).getId());
    var paused = persist(newPendingJob());
    assertTrue(store().transitionToPaused(paused.getId(), JobStatus.PENDING));
    var canceledPending = persist(newPendingJob());
    assertTrue(store().cancelJob(canceledPending.getId()));
    var pending = persist(newPendingJob());
    Map<UUID, JobStatus> expected =
        Map.of(
            pending.getId(), JobStatus.PENDING,
            running.getId(), JobStatus.RUNNING,
            paused.getId(), JobStatus.PAUSED,
            succeeded.getId(), JobStatus.SUCCEEDED,
            failed.getId(), JobStatus.FAILED,
            canceledPending.getId(), JobStatus.CANCELED,
            canceledRunning.getId(), JobStatus.CANCELED);
    assertEquals(expected, store().findStatusesByIds(expected.keySet()));
  }

  @Test
  void findStatusesByIds_missingIdsAreAbsent() {
    // Unknown ids are absent rather than assigned a synthetic status.
    var job = persist(newPendingJob());
    assertEquals(
        Map.of(job.getId(), JobStatus.PENDING),
        store().findStatusesByIds(List.of(job.getId(), UUID.randomUUID(), UUID.randomUUID())));
  }

  @Test
  void findStatusesByIds_emptyInput_returnsEmptyWithoutStoreCall() {
    // Empty status reads issue no store statements.
    assertEquals(
        0, countStoreStatements(() -> assertTrue(store().findStatusesByIds(List.of()).isEmpty())));
  }

  @Test
  void findStatusesByIds_acceptsLimitSizedChunk() {
    // The documented maximum chunk returns every requested pending job.
    List<JobEntity> jobs = new ArrayList<>(JobCrudStore.STATUS_READ_LIMIT);
    for (int i = 0; i < JobCrudStore.STATUS_READ_LIMIT; i++) {
      var job = newPendingJob();
      job.setId(UuidV7Factory.create());
      jobs.add(job);
    }
    store().bulkInsert(jobs);
    var statuses = store().findStatusesByIds(jobs.stream().map(JobEntity::getId).toList());
    assertEquals(JobCrudStore.STATUS_READ_LIMIT, statuses.size());
    for (var job : jobs) {
      assertEquals(JobStatus.PENDING, statuses.get(job.getId()));
    }
  }

  @Test
  void findStatusesByIds_overLimit_throws() {
    // Oversized chunks fail instead of silently truncating the read.
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i <= JobCrudStore.STATUS_READ_LIMIT; i++) {
      ids.add(UUID.randomUUID());
    }
    assertThrows(IllegalArgumentException.class, () -> store().findStatusesByIds(ids));
  }
}
