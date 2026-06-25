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
package run.ratchet.tck.store;

import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Per-dialect behavior for the integration testsuite's JPA helpers. Each SQL store owns one
 * implementation in its own test sources — next to its {@code DialectTypeMapper} — so the dialect
 * differences the helpers would otherwise branch on (foreign-key toggling, TRUNCATE vs DELETE, UUID
 * binding, the performance bulk insert and scan check) live with the store rather than in the
 * testsuite.
 *
 * <p>Implementations are discovered through {@link java.util.ServiceLoader}: each store registers
 * its implementation under {@code META-INF/services}, and exactly one store is on the classpath of
 * any deployment. Implementations therefore need a public no-argument constructor and must be
 * stateless.
 */
public interface SqlDialectTestSupport {

  // --- cleanup ---

  /** Suspends foreign key enforcement for the cleanup transaction. No-op where unsupported. */
  void disableForeignKeyChecks(EntityManager em);

  /** Restores foreign key enforcement after cleanup. No-op where unsupported. */
  void enableForeignKeyChecks(EntityManager em);

  /**
   * Empties one scheduler table. MySQL uses {@code TRUNCATE}; PostgreSQL and Oracle delete rows
   * instead. The scheduler poller keeps ticking through cleanup, and a row-level {@code DELETE} is
   * MVCC-friendly and respects the child-before-parent ordering the caller iterates in, so it
   * coexists with the live poller where {@code TRUNCATE} would not.
   */
  void clearTable(EntityManager em, String table);

  // --- data manipulation ---

  /**
   * Native-query parameter binding for a UUID id column. MySQL {@code BINARY(16)} and Oracle {@code
   * RAW(16)} both store the 16 big-endian bytes of the UUID, and a native query does not run the
   * entity's AttributeConverter. Hibernate happens to coerce a bare {@link UUID} parameter into
   * those bytes, but EclipseLink binds it verbatim and the predicate then matches nothing, so MySQL
   * and Oracle pre-convert. PostgreSQL is the exception: its native {@code uuid} column type
   * accepts the {@link UUID} directly.
   */
  Object jobIdParam(UUID jobId);

  // --- performance ---
  // MySQL and PostgreSQL carry the live implementations. Oracle's perf SQL lands with the perf
  // suite's hot/cold-schema migration; until then the Oracle implementation throws.

  /**
   * Inserts one chunk of background {@code scheduler_job} rows using a dialect-native bulk insert.
   */
  void insertBackgroundChunk(EntityManager em, int batchCount, int offset, String keyPrefix);

  /** Refreshes {@code scheduler_job} table statistics for accurate query-planner estimates. */
  void analyzeSchedulerJob(EntityManager em);

  /**
   * Runs {@code storeOperation} and asserts it triggered no sequential scan on {@code
   * scheduler_job}. The transaction boundaries are themselves dialect-specific — PostgreSQL reads
   * {@code pg_stat_user_tables} in separately committed transactions on either side of the
   * operation, while MySQL can only log — so each implementation owns (and rolls back) the
   * transactions it opens.
   */
  void assertNoFullScan(
      EntityManager em, UserTransaction utx, String label, Runnable storeOperation)
      throws Exception;

  /**
   * Big-endian byte encoding shared by the MySQL {@code BINARY(16)} and Oracle {@code RAW(16)} ids.
   */
  static byte[] uuidToBigEndianBytes(UUID jobId) {
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(jobId.getMostSignificantBits());
    buf.putLong(jobId.getLeastSignificantBits());
    return buf.array();
  }

  /** Best-effort rollback for implementations that open their own transactions. */
  default void rollbackQuietly(UserTransaction utx) {
    try {
      utx.rollback();
    } catch (Exception ignored) {
      // best-effort rollback
    }
  }
}
