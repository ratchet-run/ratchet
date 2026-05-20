package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;

// Verifies startup-grace gating in RecurringJobExecutor: orphaned masters are skipped until grace
// expires.
@ExtendWith(MockitoExtension.class)
class RecurringJobExecutorGraceTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobBulkStore jobBulkStore;
  @Mock private RecurringJobStore recurringJobStore;

  private RecurringRegistrationState state;
  private RecurringJobExecutor executor;

  @BeforeEach
  void setUp() {
    state = new RecurringRegistrationState();
    executor =
        new RecurringJobExecutor(
            jobBulkStore, recurringJobStore, state, () -> NodeTagFilter.NONE, FIXED_CLOCK);
  }

  @Test
  void skipsUnknownMasterWithinGraceAndReleasesClaim() {
    state.markRegistrationComplete(Set.of("known-key"));

    RecurringJobDefinition orphan = recurringMaster(42L, "orphan-key");
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(orphan));

    int fired = executor.process(10, "node-A");

    assertEquals(0, fired, "orphaned master must not fire during startup grace");

    // Grace skip is a no-op continue — the FOR UPDATE SKIP LOCKED row lock drops at tx end.
    verify(recurringJobStore, never()).advanceNextFire(any(UUID.class), any(Instant.class));
    verify(recurringJobStore, never()).cancelRecurringAndArchive(any(UUID.class), any());
    verify(jobBulkStore, never()).bulkInsert(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void firesKnownMasterWithinGrace() {
    state.markRegistrationComplete(Set.of("known-key"));

    RecurringJobDefinition known = recurringMaster(7L, "known-key");
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(known));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired);
    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    assertEquals(FIXED_NOW.plusSeconds(60), childrenCaptor.getValue().get(0).getScheduledTime());
    verify(recurringJobStore).advanceNextFire(eq(known.id()), any(Instant.class));
  }

  @Test
  void firesAnyMasterAfterGraceExpires() {
    state =
        new RecurringRegistrationState(
            RatchetOptions.builder()
                .recurring(recurring -> recurring.startupGraceSeconds(0))
                .build());
    executor =
        new RecurringJobExecutor(
            jobBulkStore, recurringJobStore, state, () -> NodeTagFilter.NONE, FIXED_CLOCK);
    state.markRegistrationComplete(Set.of("known-key"));

    RecurringJobDefinition unknown = recurringMaster(99L, "unknown-key");
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(unknown));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "after grace expires, the gate is open even for unknown keys");
  }

  @Test
  void skipReleasesEachOrphanedMasterIndependently() {
    state.markRegistrationComplete(Set.of("known"));

    RecurringJobDefinition orphan1 = recurringMaster(1L, "orphan-1");
    RecurringJobDefinition orphan2 = recurringMaster(2L, "orphan-2");
    RecurringJobDefinition known = recurringMaster(3L, "known");
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(orphan1, orphan2, known));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired);
    verify(jobBulkStore).bulkInsert(any());
    verify(recurringJobStore).advanceNextFire(eq(known.id()), any(Instant.class));
  }

  @Test
  void firesProgrammaticMasterWithNullBusinessKey() {
    state.markRegistrationComplete(Set.of("annotation-only-key"));

    RecurringJobDefinition programmatic = recurringMaster(50L, null);
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(programmatic));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "programmatic recurring jobs (null businessKey) must fire freely");
  }

  @Test
  void firesAllMastersBeforeRegistrationCompletes() {
    RecurringJobDefinition orphan = recurringMaster(11L, "any-key");
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(orphan));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "before registration the gate is permissive (programmatic-only mode)");
  }

  @Test
  void noMastersClaimedReturnsZero() {
    state.markRegistrationComplete(Set.of("any"));
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any())).thenReturn(List.of());

    int fired = executor.process(10, "node-A");

    assertEquals(0, fired);
    verify(recurringJobStore, never()).advanceNextFire(any(UUID.class), any(Instant.class));
    verify(jobBulkStore, never()).bulkInsert(any());
  }

  @Test
  void malformedCronSkipsMasterAndContinuesBatch() {
    state.markRegistrationComplete(Set.of("bad-key", "known-key"));

    RecurringJobDefinition malformed = recurringMaster(12L, "bad-key", "not a cron");
    RecurringJobDefinition known = recurringMaster(13L, "known-key");
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(malformed, known));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "malformed recurring masters must not abort the batch");
    verify(jobBulkStore).bulkInsert(any());
    verify(recurringJobStore).advanceNextFire(eq(known.id()), any(Instant.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void longDowntimeCatchupAdvancesDirectlyToNextFutureFire() {
    state.markRegistrationComplete(Set.of("known-key"));

    RecurringJobDefinition stale =
        recurringMasterWithFire(14L, "known-key", "0/1 * * * * ?", FIXED_NOW.minusSeconds(3600));
    when(recurringJobStore.claimDueRecurring(anyInt(), anyString(), any()))
        .thenReturn(List.of(stale));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired);
    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    assertEquals(11, childrenCaptor.getValue().size());
    verify(recurringJobStore, never())
        .cancelRecurringAndArchive(eq(stale.id()), eq(ArchiveReason.EXHAUSTED));
    ArgumentCaptor<Instant> next = ArgumentCaptor.forClass(Instant.class);
    verify(recurringJobStore).advanceNextFire(eq(stale.id()), next.capture());
    assertNotNull(next.getValue());
  }

  private static RecurringJobDefinition recurringMaster(long id, String businessKey) {
    return recurringMasterWithFire(id, businessKey, "0 0 12 * * ?", FIXED_NOW.plusSeconds(60));
  }

  private static RecurringJobDefinition recurringMaster(long id, String businessKey, String cron) {
    return recurringMasterWithFire(id, businessKey, cron, FIXED_NOW.plusSeconds(60));
  }

  private static RecurringJobDefinition recurringMasterWithFire(
      long id, String businessKey, String cron, Instant nextFire) {
    return new RecurringJobDefinition(
        new UUID(0L, id),
        cron,
        "UTC",
        nextFire,
        false,
        null,
        2,
        0,
        BackoffPolicy.NONE,
        0,
        0,
        null,
        null,
        null,
        null,
        businessKey,
        null,
        FIXED_NOW,
        null);
  }
}
