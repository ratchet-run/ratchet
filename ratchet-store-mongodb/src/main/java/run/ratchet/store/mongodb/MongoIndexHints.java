package run.ratchet.store.mongodb;

/**
 * Index hint names passed to {@code AggregateIterable.hintString(...)} to force the Mongo planner
 * onto the covering indexes created by {@link MongoCollectionInitializer}.
 *
 * <p>Both values must exist as index names in {@code MongoCollectionInitializer} or hinted
 * aggregations fail at runtime.
 */
final class MongoIndexHints {

  private MongoIndexHints() {}

  /** Covering index for executable job claim queries sorted by {@code scheduled_time}. */
  static final String JOB_CLAIM_EXEC = "idx_job_claim_exec";

  /** Covering index for recurring job claim queries sorted by {@code next_fire}. */
  static final String JOB_CLAIM_RECURRING = "idx_job_claim_recurring";
}
