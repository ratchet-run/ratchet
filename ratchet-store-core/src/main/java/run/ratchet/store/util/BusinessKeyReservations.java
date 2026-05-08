package run.ratchet.store.util;

import run.ratchet.store.entity.JobExecutionType;

/** Shared owner-table values for active business-key reservations. */
public final class BusinessKeyReservations {

  public static final String OWNER_TABLE_QUEUE = "QUEUE";
  public static final String OWNER_TABLE_RECURRING = "RECURRING";

  private BusinessKeyReservations() {}

  public static String ownerTableFor(JobExecutionType jobType) {
    return jobType == JobExecutionType.RECURRING ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
  }

  public static String ownerTableFor(String jobType) {
    return "RECURRING".equals(jobType) ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
  }
}
