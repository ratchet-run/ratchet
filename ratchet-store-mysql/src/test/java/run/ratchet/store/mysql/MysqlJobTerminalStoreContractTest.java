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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobTerminalStoreContract;

class MysqlJobTerminalStoreContractTest extends AbstractJobTerminalStoreContract {

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

  @Test
  void compareAndSwapStatus_cancelMissDoesNotTerminalizeCurrentStatus() {
    JobEntity saved = persist(newPendingJob());
    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));

    boolean canceled =
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);

    assertFalse(canceled, "stale expected status must not cancel a now-RUNNING job");
    assertEquals(JobStatus.RUNNING, store().findById(saved.getId()).orElseThrow().getStatus());
  }

  @Test
  void compareAndSwapStatus_cancelMatchesCurrentLiveStatusOnce() {
    JobEntity saved = persist(newPendingJob());
    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));

    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null));
    assertFalse(
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null));
    assertEquals(JobStatus.CANCELED, store().findById(saved.getId()).orElseThrow().getStatus());
  }
}
