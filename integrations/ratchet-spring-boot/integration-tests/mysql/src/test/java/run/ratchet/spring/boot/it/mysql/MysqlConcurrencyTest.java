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
package run.ratchet.spring.boot.it.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.spring.boot.it.mysql.fixture.ratchetonly.RatchetOnlyApplication;
import run.ratchet.store.mysql.MysqlJobStore;

class MysqlConcurrencyTest extends MysqlIntegrationTestSupport {

  @Test
  void parallelCommitAndRollbackWorkersDoNotBleedAcrossSharedEntityManagerProxy() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              int workers = 12;
              CountDownLatch ready = new CountDownLatch(workers);
              CountDownLatch start = new CountDownLatch(1);
              ExecutorService executor = Executors.newFixedThreadPool(workers);
              MysqlJobStore store = store(context);
              JpaTransactionManager transactionManager = transactionManager(context);
              Set<String> expectedCommitted = new HashSet<>();
              List<Future<?>> futures = new ArrayList<>();

              try {
                for (int index = 0; index < workers; index++) {
                  String idempotencyKey = UUID.randomUUID().toString();
                  boolean commit = index % 2 == 0;
                  if (commit) {
                    expectedCommitted.add(idempotencyKey);
                  }
                  futures.add(
                      executor.submit(
                          () -> {
                            ready.countDown();
                            if (!start.await(30, TimeUnit.SECONDS)) {
                              throw new IllegalStateException("Timed out awaiting worker start");
                            }
                            TransactionTemplate transaction =
                                new TransactionTemplate(transactionManager);
                            transaction.executeWithoutResult(
                                status -> {
                                  store.create(newPendingJob(idempotencyKey));
                                  if (!commit) {
                                    status.setRollbackOnly();
                                  }
                                });
                            return null;
                          }));
                }

                assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                for (Future<?> future : futures) {
                  future.get(60, TimeUnit.SECONDS);
                }
              } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
              }

              List<String> actual = queryForStrings("SELECT idempotency_key FROM scheduler_job");
              assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedCommitted);
            });
  }
}
