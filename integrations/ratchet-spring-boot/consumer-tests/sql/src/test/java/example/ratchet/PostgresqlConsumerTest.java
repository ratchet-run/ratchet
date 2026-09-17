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
package example.ratchet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.JobHandle;
import run.ratchet.consumer.sql.SqlDatabase;

class PostgresqlConsumerTest {
  @Test
  void applicationAndJobCommitAndRollbackTogether() {
    try (var database = SqlDatabase.start()) {
      try (var context =
          new SpringApplicationBuilder(ConsumerApplication.class)
              .properties(database.properties())
              .run()) {
        var service = context.getBean(ConsumerService.class);
        var jdbc = context.getBean(JdbcTemplate.class);
        var transaction =
            new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        AtomicReference<JobHandle> rollback = new AtomicReference<>();
        transaction.executeWithoutResult(
            status -> {
              rollback.set(service.submit("rollback"));
              assertThat(
                      jdbc.queryForObject(
                          "select count(*) from scheduler_job where " + database.jobIdPredicate(),
                          Integer.class,
                          database.queryId(rollback.get().id())))
                  .isEqualTo(1);
              status.setRollbackOnly();
            });
        assertThat(
                jdbc.queryForObject(
                    "select count(*) from consumer_record where id = 'rollback'", Integer.class))
            .isZero();
        assertThat(
                jdbc.queryForObject(
                    "select count(*) from scheduler_job where " + database.jobIdPredicate(),
                    Integer.class,
                    database.queryId(rollback.get().id())))
            .isZero();
        service.submit("commit");
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () ->
                    assertThat(
                            jdbc.queryForObject(
                                "select state from consumer_record where id = 'commit'",
                                String.class))
                        .isEqualTo("executed"));
      }
    }
  }
}
