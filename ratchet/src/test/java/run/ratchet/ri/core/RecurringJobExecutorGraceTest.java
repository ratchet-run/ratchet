package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

// Verifies startup-grace gating in RecurringJobExecutor: orphaned masters are skipped until grace
// expires.
@ExtendWith(MockitoExtension.class)
class RecurringJobExecutorGraceTest {

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobClaimStore jobClaimStore;
  @Mock private JobTerminalStore jobTerminalStore;

  private RecurringRegistrationState state;
  private RecurringJobExecutor executor;

  @BeforeEach
  void setUp() {
    System.clearProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY);
    state = new RecurringRegistrationState();
    executor = new RecurringJobExecutor(jobCrudStore, jobClaimStore, jobTerminalStore, state);
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY);
  }

  @Test
  void skipsUnknownMasterWithinGraceAndReleasesClaim() {
    state.markRegistrationComplete(Set.of("known-key"));

    JobEntity orphan = recurringMaster(42L, "orphan-key");
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString())).thenReturn(List.of(orphan));

    int fired = executor.process(10, "node-A");

    assertEquals(0, fired, "orphaned master must not fire during startup grace");

    // Post hot/cold-split: recurring masters have no hot row to release. The grace skip is a
    // no-op continue — the FOR UPDATE SKIP LOCKED row lock from claimDueRecurring drops at
    // tx end. No save() call is expected, and the master's next_fire stays unchanged so it's
    // eligible on the next claim cycle.
    verify(jobCrudStore, never()).save(any(JobEntity.class));
  }

  @Test
  void firesKnownMasterWithinGrace() {
    state.markRegistrationComplete(Set.of("known-key"));

    JobEntity known = recurringMaster(7L, "known-key");
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString())).thenReturn(List.of(known));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired);
    // Two saves: one for the spawned child, one for the master next_fire update.
    verify(jobCrudStore, times(2)).save(any(JobEntity.class));
  }

  @Test
  void firesAnyMasterAfterGraceExpires() {
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "0");
    state.markRegistrationComplete(Set.of("known-key"));

    JobEntity unknown = recurringMaster(99L, "unknown-key");
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString())).thenReturn(List.of(unknown));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "after grace expires, the gate is open even for unknown keys");
  }

  @Test
  void skipReleasesEachOrphanedMasterIndependently() {
    state.markRegistrationComplete(Set.of("known"));

    JobEntity orphan1 = recurringMaster(1L, "orphan-1");
    JobEntity orphan2 = recurringMaster(2L, "orphan-2");
    JobEntity known = recurringMaster(3L, "known");
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString()))
        .thenReturn(List.of(orphan1, orphan2, known));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired);
    // Post hot/cold-split: orphans skip without save() (no hot row to release). Only the
    // known master triggers two saves: one for the spawned child, one for the next_fire
    // cold-only update on the master.
    verify(jobCrudStore, times(2)).save(any(JobEntity.class));
  }

  @Test
  void firesProgrammaticMasterWithNullBusinessKey() {
    state.markRegistrationComplete(Set.of("annotation-only-key"));

    // Programmatically-submitted recurring jobs may have null business key.
    JobEntity programmatic = recurringMaster(50L, null);
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString())).thenReturn(List.of(programmatic));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "programmatic recurring jobs (null businessKey) must fire freely");
  }

  @Test
  void firesAllMastersBeforeRegistrationCompletes() {
    // markRegistrationComplete never called — registration hasn't run.
    JobEntity orphan = recurringMaster(11L, "any-key");
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString())).thenReturn(List.of(orphan));

    int fired = executor.process(10, "node-A");

    assertEquals(1, fired, "before registration the gate is permissive (programmatic-only mode)");
  }

  @Test
  void noMastersClaimedReturnsZero() {
    state.markRegistrationComplete(Set.of("any"));
    when(jobClaimStore.claimDueRecurring(anyInt(), anyString())).thenReturn(List.of());

    int fired = executor.process(10, "node-A");

    assertEquals(0, fired);
    verify(jobCrudStore, never()).save(any(JobEntity.class));
  }

  private JobEntity recurringMaster(long id, String businessKey) {
    JobEntity master = new JobEntity();
    master.setId(id);
    master.setBusinessKey(businessKey);
    master.setJobType(JobExecutionType.RECURRING);
    master.setStatus(JobStatus.PENDING);
    master.setCronExpr("0 0 12 * * ?"); // noon daily
    master.setZoneId("UTC");
    master.setNextFire(Instant.now().plusSeconds(60));
    master.setPickedBy("node-A");
    master.setPickedAt(Instant.now());
    return master;
  }
}
