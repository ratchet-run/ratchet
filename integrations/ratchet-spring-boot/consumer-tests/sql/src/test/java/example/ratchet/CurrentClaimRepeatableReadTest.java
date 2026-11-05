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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.orm.jpa.EntityManagerFactoryUtils;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.spi.JobStore;

/** Tests fresh claim visibility through the application's transactional store proxy. */
class CurrentClaimRepeatableReadTest {
  @Test
  void currentClaimSeesReclaimOutsideRepeatableReadSnapshot() throws Exception {
    assumeTrue("mysql".equals(System.getProperty("store", "postgresql")), "MySQL only");
    try (var database = SqlDatabase.start();
        var context =
            new SpringApplicationBuilder(ConsumerApplication.class)
                .properties(database.properties())
                .properties("ratchet.node.orphan-grace-seconds=3600")
                .run()) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var jobs = context.getBean(ConsumerJobs.class);
      var store = context.getBean(JobStore.class);
      var manager = context.getBean(PlatformTransactionManager.class);
      var factory = context.getBean(EntityManagerFactory.class);
      // Future scheduling keeps the poller away; explicit pickup does not require the job to be
      // due.
      String record = "repeatable-read-" + UUID.randomUUID();
      UUID id = scheduler.schedule(Duration.ofHours(2), () -> jobs.complete(record)).submit().id();
      var writer = Executors.newSingleThreadExecutor();
      try {
        assertThat(store.tryPickUpJob(id, "snapshot-owner")).isTrue();
        long original = store.currentClaim(id).orElseThrow().claimSeq();
        var outer = new TransactionTemplate(manager);
        outer.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        outer.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        try {
          outer.executeWithoutResult(
              status -> {
                EntityManager em = EntityManagerFactoryUtils.getTransactionalEntityManager(factory);
                assertThat(em).isNotNull();
                String isolation =
                    em.createNativeQuery("SELECT @@transaction_isolation")
                        .getSingleResult()
                        .toString();
                assumeTrue(
                    "REPEATABLE-READ".equalsIgnoreCase(isolation),
                    "Provider did not apply REPEATABLE READ: " + isolation);
                assertThat(sequence(em, id)).isEqualTo(original);
                var reclaimed =
                    writer.submit(
                        () -> {
                          var transaction = new TransactionTemplate(manager);
                          transaction.setPropagationBehavior(
                              TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                          return transaction.execute(
                              tx -> {
                                assertThat(store.resetRunningJob(id, "snapshot-owner", original))
                                    .isTrue();
                                assertThat(store.tryPickUpJob(id, "new-owner")).isTrue();
                                // Read the writer's own change; currentClaim deliberately uses
                                // another transaction.
                                var writerEm =
                                    EntityManagerFactoryUtils.getTransactionalEntityManager(
                                        factory);
                                return sequence(writerEm, id);
                              });
                        });
                long latest;
                try {
                  latest = reclaimed.get(10, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError("Interrupted waiting for reclaim", interrupted);
                } catch (Exception failure) {
                  throw new AssertionError("Reclaim failed", failure);
                }
                assertThat(latest).isGreaterThan(original);
                assertThat(store.currentClaim(id).orElseThrow().claimSeq()).isEqualTo(latest);
                assertThat(sequence(em, id)).isEqualTo(original);
              });
        } catch (InvalidIsolationLevelException unsupported) {
          assumeTrue(
              false, "Provider cannot set transaction isolation: " + unsupported.getMessage());
        }
      } finally {
        writer.shutdownNow();
        try {
          assertThat(writer.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        } finally {
          store.delete(id);
        }
      }
    }
  }

  private static long sequence(EntityManager em, UUID id) {
    return ((Number)
            em.createNativeQuery(
                    "SELECT claim_seq FROM scheduler_job_queue WHERE job_id = UUID_TO_BIN(?1)")
                .setParameter(1, id.toString())
                .getSingleResult())
        .longValue();
  }
}
