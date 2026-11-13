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
package run.ratchet.store.postgresql;

import java.time.Duration;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractBusinessKeyReservationContract;
import run.ratchet.tck.store.JdbcLockWait;

class PostgresqlBusinessKeyReservationContractTest extends AbstractBusinessKeyReservationContract {

  private final PostgresqlTestFixture fixture = new PostgresqlTestFixture();

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
    return fixture.currentSessionId("SELECT pg_backend_pid()");
  }

  @Override
  public boolean awaitBlockedSession(Object waiter, Object holder, Duration timeout) {
    return JdbcLockWait.awaitBlockedSession(
        fixture::openConnection,
        "SELECT COUNT(*) FROM pg_stat_activity WHERE pid = ? AND ? = ANY (pg_blocking_pids(pid))",
        timeout,
        ((Number) waiter).intValue(),
        ((Number) holder).intValue());
  }
}
