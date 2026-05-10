package run.ratchet.api;

/** Incubating fields on which a {@link JobQueryService} result set may be sorted. */
@Incubating
public enum JobQuerySortField {
  CREATED_AT,
  SCHEDULED_TIME,
  UPDATED_AT,
  PRIORITY,
  STATUS
}
