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
import java.time.Duration;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractIdempotencyKeyReservationContract;
import run.ratchet.tck.store.JdbcLockWait;

class MysqlIdempotencyKeyReservationContractTest extends AbstractIdempotencyKeyReservationContract {

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
  protected boolean plainReadsUseTransactionSnapshot() {
    return !"2".equals(System.getProperty("ratchet.mysql.test.isolation", "4"));
  }
}
