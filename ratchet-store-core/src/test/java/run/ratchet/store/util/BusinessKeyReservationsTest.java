package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobExecutionType;

class BusinessKeyReservationsTest {

  @Test
  void ownerTableForJobExecutionTypeUsesRecurringOnlyForRecurringJobs() {
    assertEquals(
        BusinessKeyReservations.OWNER_TABLE_RECURRING,
        BusinessKeyReservations.ownerTableFor(JobExecutionType.RECURRING));
    assertEquals(
        BusinessKeyReservations.OWNER_TABLE_QUEUE,
        BusinessKeyReservations.ownerTableFor(JobExecutionType.SINGLE));
  }

  @Test
  void ownerTableForStringMatchesPersistedRecurringValueOnly() {
    assertEquals(
        BusinessKeyReservations.OWNER_TABLE_RECURRING,
        BusinessKeyReservations.ownerTableFor("RECURRING"));
    assertEquals(
        BusinessKeyReservations.OWNER_TABLE_QUEUE, BusinessKeyReservations.ownerTableFor("SINGLE"));
    assertEquals(
        BusinessKeyReservations.OWNER_TABLE_QUEUE,
        BusinessKeyReservations.ownerTableFor((String) null));
  }
}
