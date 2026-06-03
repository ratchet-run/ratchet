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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractBatchStoreContract;
import run.ratchet.tck.util.ConcurrentTestRunner;

class MysqlBatchStoreContractTest extends AbstractBatchStoreContract {

  private final Fixture fixture = new Fixture();

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
  void incrementCompletedAtomic_failsOnCorruptProgressHookPayload() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);
    fixture.corruptProgressHook(parent.getId());

    assertThrows(
        IllegalArgumentException.class,
        () -> batchStore().incrementCompletedAtomic(parent.getId()));
  }

  @Test
  void incrementAtomic_preservesMixedConcurrentIncrementsAtReadCommitted() {
    int completedCount = 12;
    int failedCount = 8;
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), completedCount + failedCount);

    Runnable[] tasks = new Runnable[completedCount + failedCount];
    for (int i = 0; i < completedCount; i++) {
      tasks[i] = () -> batchStore().incrementCompletedAtomic(parent.getId());
    }
    for (int i = completedCount; i < tasks.length; i++) {
      tasks[i] = () -> batchStore().incrementFailedAtomic(parent.getId());
    }

    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    var batch = batchStore().findBatchById(parent.getId()).orElseThrow();
    assertEquals(completedCount, batch.getCompletedItems());
    assertEquals(failedCount, batch.getFailedItems());
  }

  private static final class Fixture extends MysqlTestFixture {
    void corruptProgressHook(UUID batchId) {
      String batchIdHex = batchId.toString().replace("-", "");
      executeNativeSql(
          "UPDATE scheduler_batch SET progress_hook = CAST('\"not-a-job-payload\"' AS JSON) "
              + "WHERE batch_id = UNHEX('"
              + batchIdHex
              + "')");
    }
  }
}
