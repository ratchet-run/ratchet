package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecurringRegistrationStateTest {

  private RecurringRegistrationState state;

  @BeforeEach
  void setUp() {
    state = new RecurringRegistrationState();
    System.clearProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY);
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY);
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
    // Programmatically-submitted recurring jobs may not have a business key. They were not
    // declared via @Recurring so the orphan cleanup never targets them — fire freely.
    state.markRegistrationComplete(Set.of("alpha"));
    assertTrue(state.shouldFire(null));
  }

  @Test
  void shouldFireReturnsTrueAfterGraceExpires() {
    // Use a 0-second grace via system property to simulate post-grace state immediately.
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "0");
    state.markRegistrationComplete(Set.of("alpha"));
    // Even an unknown key fires because the grace window is 0.
    assertTrue(state.shouldFire("orphan"));
  }

  @Test
  void inStartupGraceIsFalseWhenGraceIsZero() {
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "0");
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
    assertEquals(60L, RecurringRegistrationState.startupGraceSeconds());
  }

  @Test
  void startupGraceSecondsHonorsSystemProperty() {
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "30");
    assertEquals(30L, RecurringRegistrationState.startupGraceSeconds());
  }

  @Test
  void startupGraceSecondsClampsNegativeToZero() {
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "-5");
    assertEquals(0L, RecurringRegistrationState.startupGraceSeconds());
  }

  @Test
  void startupGraceSecondsFallsBackOnInvalidProperty() {
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "not-a-number");
    assertEquals(60L, RecurringRegistrationState.startupGraceSeconds());
  }

  @Test
  void startupGraceSecondsTreatsBlankAsUnset() {
    System.setProperty(RecurringRegistrationState.STARTUP_GRACE_PROPERTY, "   ");
    assertEquals(60L, RecurringRegistrationState.startupGraceSeconds());
  }
}
