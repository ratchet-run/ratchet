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
package run.ratchet.spring.boot.it.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.spring.boot.it.sqlserver.fixture.ratchetonly.RatchetOnlyApplication;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.sqlserver.SqlserverJobStore;

class SqlserverTransactionTruthTest extends SqlserverIntegrationTestSupport {

  @Test
  void transactionTemplateCommitPersistsJobRow() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              SqlserverJobStore store = store(context);
              TransactionTemplate transaction =
                  new TransactionTemplate(transactionManager(context));
              String idempotencyKey = UUID.randomUUID().toString();
              AtomicReference<JobEntity> persisted = new AtomicReference<>();

              transaction.executeWithoutResult(
                  status -> persisted.set(store.create(newPendingJob(idempotencyKey))));

              assertThat(persisted.get().getId()).isNotNull();
              assertThat(
                      queryForLong(
                          "SELECT COUNT(*) FROM scheduler_job WHERE idempotency_key = '"
                              + idempotencyKey
                              + "'"))
                  .isEqualTo(1L);
            });
  }

  @Test
  void transactionTemplateRollbackLeavesNoJobRow() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              SqlserverJobStore store = store(context);
              TransactionTemplate transaction =
                  new TransactionTemplate(transactionManager(context));
              String idempotencyKey = UUID.randomUUID().toString();

              transaction.executeWithoutResult(
                  status -> {
                    store.create(newPendingJob(idempotencyKey));
                    status.setRollbackOnly();
                  });

              assertThat(
                      queryForLong(
                          "SELECT COUNT(*) FROM scheduler_job WHERE idempotency_key = '"
                              + idempotencyKey
                              + "'"))
                  .isZero();
            });
  }

  @Test
  void storeRequiresNewHeartbeatSurvivesOuterRollback() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              SqlserverJobStore store = store(context);
              TransactionTemplate outer = new TransactionTemplate(transactionManager(context));
              String nodeId = "requires-new-" + UUID.randomUUID();

              outer.executeWithoutResult(
                  status -> {
                    store.upsertHeartbeat(nodeId, Instant.now());
                    status.setRollbackOnly();
                  });

              assertThat(
                      queryForLong(
                          "SELECT COUNT(*) FROM scheduler_node WHERE node_id = '" + nodeId + "'"))
                  .isEqualTo(1L);
            });
  }
}
