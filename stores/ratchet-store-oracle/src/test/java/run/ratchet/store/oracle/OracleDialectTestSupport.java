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
package run.ratchet.store.oracle;

import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.UUID;
import run.ratchet.tck.store.SqlDialectTestSupport;

/**
 * Oracle {@link SqlDialectTestSupport}: RAW(16) ids, no foreign-key toggling, and row-level DELETE.
 *
 * <p>The performance methods throw {@link UnsupportedOperationException}: the perf helper's bulk
 * insert and scan checks still target the pre-hot/cold-split {@code scheduler_job} columns and run
 * only on MySQL and PostgreSQL today. Oracle's perf SQL arrives with the perf suite's hot/cold
 * migration; the cleanup and id-binding paths used by every functional IT are fully supported here.
 */
public final class OracleDialectTestSupport implements SqlDialectTestSupport {

  private static final String PERF_PENDING =
      "Oracle performance SQL lands with the perf suite's hot/cold-schema migration";

  /** Public no-arg constructor required by {@link java.util.ServiceLoader}. */
  public OracleDialectTestSupport() {}

  @Override
  public void disableForeignKeyChecks(EntityManager em) {
    // Oracle has no session-level foreign-key toggle; DELETE respects the child-before-parent
    // ordering the caller iterates in.
  }

  @Override
  public void enableForeignKeyChecks(EntityManager em) {
    // No-op — see disableForeignKeyChecks.
  }

  @Override
  public void clearTable(EntityManager em, String table) {
    // A concurrent TRUNCATE both fails outright on tables that enabled foreign keys reference
    // (ORA-02266) and resets a table's data-object number, so any in-flight poller query against it
    // dies with ORA-08103. Row-level DELETE is MVCC-friendly and coexists with the live poller.
    em.createNativeQuery("DELETE FROM " + table).executeUpdate();
  }

  @Override
  public Object jobIdParam(UUID jobId) {
    return SqlDialectTestSupport.uuidToBigEndianBytes(jobId);
  }

  @Override
  public void insertBackgroundChunk(
      EntityManager em, int batchCount, int offset, String keyPrefix) {
    throw new UnsupportedOperationException(PERF_PENDING);
  }

  @Override
  public void analyzeSchedulerJob(EntityManager em) {
    throw new UnsupportedOperationException(PERF_PENDING);
  }

  @Override
  public void assertNoFullScan(
      EntityManager em, UserTransaction utx, String label, Runnable storeOperation) {
    throw new UnsupportedOperationException(PERF_PENDING);
  }
}
