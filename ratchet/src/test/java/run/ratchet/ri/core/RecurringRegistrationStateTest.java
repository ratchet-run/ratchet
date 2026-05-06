package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;

class RecurringRegistrationStateTest {

  private RecurringRegistrationState state;

  @BeforeEach
  void setUp() {
    state = new RecurringRegistrationState();
  }

  @Test
  void shouldFireReturnsTrueBeforeRegistrationCompletes() {
    // Programmatic-only deployments may never call markRegistrationComplete. Be permissive.
    assertTrue(state.shouldFire("any-key"));
    assertTrue(state.shouldFire(null));
  }

  @Test
  void inStartupGraceIsFalseBeforeRegistration() {
    assertFalse(state.inStartupGrace());
  }

  @Test
  void shouldFireReturnsTrueForKnownKeyDuringGrace() {
    state.markRegistrationComplete(Set.of("alpha", "beta"));
    assertTrue(state.shouldFire("alpha"));
    assertTrue(state.shouldFire("beta"));
  }

  @Test
  void shouldFireReturnsFalseForUnknownKeyDuringGrace() {
    state.markRegistrationComplete(Set.of("alpha"));
    assertFalse(state.shouldFire("orphan"));
  }

  @Test
  void shouldFireReturnsTrueForNullBusinessKeyDuringGrace() {
    state.markRegistrationComplete(Set.of("alpha"));
    assertTrue(state.shouldFire(null));
  }

  @Test
  void shouldFireReturnsTrueAfterGraceExpires() {
    state =
        new RecurringRegistrationState(
            RatchetOptions.builder()
                .recurring(recurring -> recurring.startupGraceSeconds(0))
                .build());
    state.markRegistrationComplete(Set.of("alpha"));
    // Even an unknown key fires because the grace window is 0.
    assertTrue(state.shouldFire("orphan"));
  }

  @Test
  void inStartupGraceIsFalseWhenGraceIsZero() {
    state =
        new RecurringRegistrationState(
            RatchetOptions.builder()
                .recurring(recurring -> recurring.startupGraceSeconds(0))
                .build());
    state.markRegistrationComplete(Set.of("alpha"));
    assertFalse(state.inStartupGrace());
  }

  @Test
  void inStartupGraceIsTrueImmediatelyAfterRegistration() {
    state.markRegistrationComplete(Set.of("alpha"));
    assertTrue(state.inStartupGrace());
  }

  @Test
  void markRegistrationCompleteSetsTimestamp() {
    assertNull(state.registrationCompletedAt());
    state.markRegistrationComplete(Set.of());
    assertNotNull(state.registrationCompletedAt());
  }

  @Test
  void markRegistrationCompleteReplacesExistingKeys() {
    state.markRegistrationComplete(Set.of("alpha", "beta"));
    state.markRegistrationComplete(Set.of("gamma"));
    assertEquals(Set.of("gamma"), state.snapshotKnownKeys());
  }

  @Test
  void markRegistrationCompleteAcceptsEmptySet() {
    state.markRegistrationComplete(Set.of());
    assertNotNull(state.registrationCompletedAt());
    // With an empty known set, no key passes the gate during grace.
    assertFalse(state.shouldFire("anything"));
  }

  @Test
  void startupGraceSecondsDefaultsTo60() {
    assertEquals(60L, state.startupGraceSeconds());
  }

  @Test
  void startupGraceSecondsHonorsRatchetOptions() {
    state =
        new RecurringRegistrationState(
            RatchetOptions.builder()
                .recurring(recurring -> recurring.startupGraceSeconds(30))
                .build());
    assertEquals(30L, state.startupGraceSeconds());
  }
}
