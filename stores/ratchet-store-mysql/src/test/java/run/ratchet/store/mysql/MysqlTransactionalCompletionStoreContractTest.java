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
package run.ratchet.store.mysql;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractTransactionalCompletionStoreContract;
import run.ratchet.tck.store.JdbcLockWait;

class MysqlTransactionalCompletionStoreContractTest
    extends AbstractTransactionalCompletionStoreContract {

  private final MysqlTestFixture fixture = new MysqlTestFixture();

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }

  @Override
  public void inOwnerTransaction(Runnable work) {
    fixture.runInTransaction(work);
  }

  @Override
  public boolean supportsOwnerTransaction() {
    return true;
  }

  @Override
  public Object ownerSessionId() {
    return fixture.currentSessionId("SELECT CONNECTION_ID()");
  }

  @Override
  public boolean awaitBlockedSession(Object waiter, Object holder, Duration timeout) {
    // Root is needed because the view reads INNODB_TRX.
    return JdbcLockWait.awaitBlockedSession(
        () ->
            DriverManager.getConnection(
                MysqlTestFixture.jdbcUrl(), "root", MysqlTestFixture.password()),
        "SELECT COUNT(*) FROM sys.innodb_lock_waits WHERE waiting_pid = ? AND blocking_pid = ?",
        timeout,
        waiter,
        holder);
  }

  @Override
  public int countStoreStatements(Runnable work) {
    return fixture.countStoreStatements(work);
  }

  @Override
  public void inOwnerTransactionAtSnapshotIsolation(Runnable work) {
    fixture.runAtSnapshotIsolation(work);
  }

  @Override
  public boolean queueRowUpdateLockRefused(UUID jobId) {
    try (var connection = fixture.openConnection()) {
      connection.setAutoCommit(false);
      try {
        try (var statement =
            connection.prepareStatement(
                "SELECT 1 FROM scheduler_job_queue WHERE job_id = ? FOR SHARE NOWAIT")) {
          statement.setBytes(1, UuidByteArrayConverter.toBytes(jobId));
          try (var rows = statement.executeQuery()) {
            if (!rows.next()) throw new IllegalStateException("Queue row missing: " + jobId);
            return false;
          }
        } catch (SQLException failure) {
          if (failure.getErrorCode() == 3572) return true;
          throw failure;
        }
      } finally {
        connection.rollback();
      }
    } catch (SQLException failure) {
      throw new IllegalStateException("Queue-row lock probe failed for " + jobId, failure);
    }
  }
}
