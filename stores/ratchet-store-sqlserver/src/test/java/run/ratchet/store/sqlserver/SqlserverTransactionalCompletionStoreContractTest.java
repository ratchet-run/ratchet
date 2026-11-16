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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.tck.store.AbstractTransactionalCompletionStoreContract;
import run.ratchet.tck.store.JdbcLockWait;

class SqlserverTransactionalCompletionStoreContractTest
    extends AbstractTransactionalCompletionStoreContract {

  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

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
    return fixture.currentSessionId("SELECT CAST(@@SPID AS INT)");
  }

  @Override
  public boolean awaitBlockedSession(Object waiter, Object holder, Duration timeout) {
    return JdbcLockWait.awaitBlockedSession(
        fixture::openConnection,
        "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id = ? AND blocking_session_id ="
            + " ?",
        timeout,
        ((Number) waiter).intValue(),
        ((Number) holder).intValue());
  }

  @Override
  public int countStoreStatements(Runnable work) {
    return fixture.countStoreStatements(work);
  }

  @Override
  public void inOwnerTransactionAtSnapshotIsolation(Runnable work) {
    enableSnapshotIsolation();
    fixture.runInTransaction(
        () -> {
          // A SET sent as a parameterized RPC is undone when the RPC returns, so a native query
          // cannot set the level. Set it on the connection, then confirm it holds.
          fixture.setCurrentIsolation(SQLServerConnection.TRANSACTION_SNAPSHOT);
          assertEquals(
              SNAPSHOT_LEVEL,
              fixture.currentSessionId(
                  """
                  SELECT CAST(transaction_isolation_level AS INT)
                  FROM sys.dm_exec_sessions WHERE session_id = @@SPID
                  """),
              "The owner transaction must run at SNAPSHOT isolation");
          try {
            work.run();
          } finally {
            fixture.setCurrentIsolation(Connection.TRANSACTION_READ_COMMITTED);
          }
        });
  }

  // sys.dm_exec_sessions.transaction_isolation_level reports SNAPSHOT as 5.
  private static final long SNAPSHOT_LEVEL = 5;

  private static boolean snapshotEnabled;

  private void enableSnapshotIsolation() {
    synchronized (SqlserverTransactionalCompletionStoreContractTest.class) {
      if (snapshotEnabled) {
        return;
      }
      try (var connection = fixture.openConnection();
          var statement = connection.createStatement()) {
        statement.execute("ALTER DATABASE ratchet SET ALLOW_SNAPSHOT_ISOLATION ON");
        snapshotEnabled = true;
      } catch (SQLException e) {
        throw new IllegalStateException("Could not enable snapshot isolation", e);
      }
    }
  }

  @Override
  public boolean isSerializationFailure(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql && sql.getErrorCode() == 3960) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean queueRowUpdateLockRefused(UUID jobId) {
    try (var connection = fixture.openConnection()) {
      connection.setAutoCommit(false);
      try {
        try (var statement = connection.createStatement()) {
          statement.execute("SET LOCK_TIMEOUT 0");
        }
        try (var statement =
            connection.prepareStatement(
                "SELECT 1 FROM scheduler_job_queue WITH (UPDLOCK, ROWLOCK) WHERE job_id = ?")) {
          statement.setBytes(1, UuidByteArrayConverter.toBytes(jobId));
          try (var rows = statement.executeQuery()) {
            if (!rows.next()) throw new IllegalStateException("Queue row missing: " + jobId);
            return false;
          }
        } catch (SQLException failure) {
          if (failure.getErrorCode() == 1222) return true;
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
