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
package run.ratchet.store.mongodb;

/**
 * Index hint names passed to {@code AggregateIterable.hintString(...)} to force the Mongo planner
 * onto the covering indexes created by {@link MongoCollectionInitializer}.
 *
 * <p>Every value must exist as an index name in {@code MongoCollectionInitializer} or hinted
 * aggregations fail at runtime.
 */
final class MongoIndexHints {

  /** Covering index for executable job claim queries sorted by {@code scheduled_time}. */
  static final String JOB_CLAIM_EXEC = "idx_job_claim_exec";

  /**
   * Covering claim index on the dedicated {@code scheduler_recurring_job} collection, filtered by
   * {@code is_paused} and sorted by {@code next_fire}.
   */
  static final String RECURRING_JOB_CLAIM = "idx_rec_claim";

  private MongoIndexHints() {}
}
