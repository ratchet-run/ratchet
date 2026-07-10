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
package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/**
 * JPA/SQL implementation of {@link TestDataManipulator}.
 *
 * <p>Uses native SQL queries within interceptor-managed transactions to manipulate test data. Only
 * packaged in the WAR when a JPA store profile is active.
 *
 * <p>These methods deliberately use {@code @Transactional(REQUIRES_NEW)} rather than a direct
 * {@link jakarta.transaction.UserTransaction}: bare UserTransaction calls are gated by a
 * TransactionOperationsManager check that a Payara/GlassFish {@code TransactionalInterceptorBase}
 * race can corrupt under concurrent load, while the interceptor path never consults that gate. See
 * {@code run.ratchet.testsuite.diagnostics.UtxTomRaceStressIT}.
 */
@ApplicationScoped
public class JpaTestDataManipulator implements TestDataManipulator {

  @Inject private RatchetEntityManagerProvider entityManagerProvider;

  private static Object jobIdParam(UUID jobId) {
    return SqlDialectTestSupportProvider.get().jobIdParam(jobId);
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void setJobUpdatedAt(UUID jobId, Instant updatedAt) {
    Timestamp ts = Timestamp.from(updatedAt);

    // Both JPA stores are now hot/cold-split: cold scheduler_job has no updated_at; the
    // archive/DLQ-purge cutoff lives on cold.terminated_at, and the live update timestamp
    // lives on scheduler_job_queue.updated_at. Tests aim this method at one or the other
    // depending on the row's lifecycle stage.
    // language=SQL
    String coldSql =
        """
        UPDATE scheduler_job SET terminated_at = ?1
        WHERE job_id = ?2 AND terminal_status IS NOT NULL
        """;
    Object idParam = jobIdParam(jobId);
    em().createNativeQuery(coldSql).setParameter(1, ts).setParameter(2, idParam).executeUpdate();
    try {
      // language=SQL
      String hotSql = "UPDATE scheduler_job_queue SET updated_at = ?1 WHERE job_id = ?2";
      em().createNativeQuery(hotSql).setParameter(1, ts).setParameter(2, idParam).executeUpdate();
    } catch (RuntimeException ignored) {
      // The queue row may not exist once a job has moved to the terminal table.
    }
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void setArchivedAt(UUID archiveId, Instant archivedAt) {
    Timestamp ts = Timestamp.from(archivedAt);

    // language=SQL
    String sql = "UPDATE scheduler_job_archive SET archived_at = ?1 WHERE archive_id = ?2";
    em().createNativeQuery(sql)
        .setParameter(1, ts)
        .setParameter(2, jobIdParam(archiveId))
        .executeUpdate();
  }

  private EntityManager em() {
    return entityManagerProvider.getEntityManager();
  }
}
