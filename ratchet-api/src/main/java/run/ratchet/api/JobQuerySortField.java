package run.ratchet.api;

/** Fields on which a {@link JobQueryService} result set may be sorted. */
public enum JobQuerySortField {
  CREATED_AT,
  SCHEDULED_TIME,
  UPDATED_AT,
  PRIORITY,
  STATUS
}
