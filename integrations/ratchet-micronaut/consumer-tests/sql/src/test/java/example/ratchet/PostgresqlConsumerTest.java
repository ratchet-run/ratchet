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
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.consumer.sql.SqlAccess;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;

class PostgresqlConsumerTest {
  @Test
  void applicationAndJobCommitAndRollbackTogether() {
    try (var database = SqlDatabase.start()) {
      try (var context = SqlApplication.start(database)) {
        var service = context.getBean(ConsumerService.class);
        var jdbc = new SqlAccess(context);
        var transaction = new SqlAccess(context);
        AtomicReference<JobHandle> rollback = new AtomicReference<>();
        transaction.transaction(
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

  @Test
  void recurringBeanWorkflowAndSignalRunThroughTheConsumer() {
    ConsumerRecurringJobs.EXECUTIONS.set(0);
    try (var database = SqlDatabase.start();
        var context = SqlApplication.start(database, "consumer.recurring=true")) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var service = context.getBean(ConsumerService.class);
      var jobs = context.getBean(ConsumerJobs.class);
      var jdbc = new SqlAccess(context);
      jdbc.update("insert into consumer_record (id, state) values (?, ?)", "chain-a", "submitted");
      jdbc.update("insert into consumer_record (id, state) values (?, ?)", "chain-b", "submitted");
      jdbc.update("insert into consumer_record (id, state) values (?, ?)", "signal", "submitted");
      service.submit("normal");
      scheduler
          .enqueue(() -> jobs.complete("chain-a"))
          .then(() -> jobs.complete("chain-b"))
          .submit();
      var waiter =
          scheduler
              .enqueue(() -> jobs.complete("signal"))
              .awaitSignal("consumer-signal", Duration.ofMinutes(1))
              .submit();
      assertThat(
              jdbc.queryForObject(
                  "select state from consumer_record where id = 'signal'", String.class))
          .isEqualTo("submitted");
      assertThat(scheduler.deliverSignal(waiter.id(), "go")).isEqualTo(1);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                for (String id : new String[] {"normal", "chain-a", "chain-b", "signal"})
                  assertThat(
                          jdbc.queryForObject(
                              "select state from consumer_record where id = ?", String.class, id))
                      .isEqualTo("executed");
                assertThat(ConsumerRecurringJobs.EXECUTIONS.get()).isPositive();
                assertThat(
                        jdbc.queryForObject(
                            "select count(*) from scheduler_recurring_job where business_key ="
                                + " 'consumer-recurring'",
                            Integer.class))
                    .isEqualTo(1);
              });
    }
  }
}
