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
  // The schema splits completed jobs (cold {@code scheduler_job}) from live, claimable jobs
  // (hot {@code scheduler_job_queue}). The two perf ITs grow different tables: table-growth grows
  // the cold archive, claim-degradation grows the hot queue. Each store owns the dialect-native
  // bulk insert and the (dialect-specific) scan diagnostics for both.

  /**
   * Inserts one chunk of terminal (completed) {@code scheduler_job} rows — {@code
   * terminal_status='SUCCEEDED'}, no queue row — so they grow the cold archive without becoming
   * claimable. Used by the table-growth IT. Random ids; {@code offset} keeps {@code business_key}
   * and {@code idempotency_key} unique across chunks.
   */
  void insertTerminalChunk(EntityManager em, int batchCount, int offset, String keyPrefix);

  /**
   * Inserts one chunk of live, PENDING jobs: a cold {@code scheduler_job} parent plus a hot {@code
   * scheduler_job_queue} row referencing it (the queue's foreign key requires the parent). The
   * queue rows are far-future {@code scheduled_time} so the running poller never claims or drains
   * them mid-measurement. Used by the claim-degradation IT. Ids are derived deterministically from
   * {@code offset + row} so the cold and hot inserts agree on {@code job_id} without an anti-join.
   */
  void insertPendingQueueChunk(EntityManager em, int batchCount, int offset, String keyPrefix);

  /** Refreshes one table's statistics for accurate query-planner estimates. */
  void analyzeTable(EntityManager em, String table);

  /**
   * Runs {@code storeOperation} and asserts it triggered no sequential scan on {@code table}. The
   * transaction boundaries are themselves dialect-specific — PostgreSQL reads {@code
   * pg_stat_user_tables} in separately committed transactions on either side of the operation,
   * while MySQL and Oracle can only log — so each implementation owns (and rolls back) the
   * transactions it opens.
   */
  void assertNoFullScan(
      EntityManager em, UserTransaction utx, String table, String label, Runnable storeOperation)
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
