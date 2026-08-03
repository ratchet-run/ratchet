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

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.spring.boot.it.sqlserver.fixture.application.ApplicationEntityApplication;
import run.ratchet.spring.boot.it.sqlserver.fixture.application.ConsumerNote;
import run.ratchet.store.entity.JobEntity;

class SqlserverApplicationEntityTopologyTest extends SqlserverIntegrationTestSupport {

  @Test
  void applicationAndRatchetEntitiesShareOnePersistenceUnitAndTransactionManager()
      throws Exception {
    executeSql(
        "CREATE TABLE consumer_note (id BINARY(16) PRIMARY KEY, message NVARCHAR(255) NOT NULL)");

    contextRunner(ApplicationEntityApplication.class, migrationOptions(""))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertSingleJpaTopology(context);

              EntityManagerFactory entityManagerFactory = entityManagerFactory(context);
              assertThat(entityManagerFactory.getMetamodel().entity(JobEntity.class)).isNotNull();
              assertThat(entityManagerFactory.getMetamodel().entity(ConsumerNote.class))
                  .isNotNull();

              UUID id = UUID.randomUUID();
              String idempotencyKey = UUID.randomUUID().toString();
              EntityManager entityManager =
                  SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
              TransactionTemplate transaction =
                  new TransactionTemplate(transactionManager(context));
              transaction.executeWithoutResult(
                  status -> {
                    entityManager.persist(new ConsumerNote(id, "same persistence unit"));
                    store(context).create(newPendingJob(idempotencyKey));
                  });

              assertThat(
                      queryForLong(
                          "SELECT COUNT(*) FROM consumer_note WHERE id = 0x"
                              + id.toString().replace("-", "")))
                  .isEqualTo(1L);
              assertThat(
                      queryForLong(
                          "SELECT COUNT(*) FROM scheduler_job WHERE idempotency_key = '"
                              + idempotencyKey
                              + "'"))
                  .isEqualTo(1L);
            });
  }
}
