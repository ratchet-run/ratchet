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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import run.ratchet.store.spi.RatchetEntityManagerProvider;
import run.ratchet.tck.store.SqlDialectTestSupport;

/**
 * JPA/SQL implementation of {@link TestCleanupStrategy}.
 *
 * <p>Clears all scheduler tables using native SQL within a JTA transaction. The dialect-specific
 * pieces — foreign-key toggling and TRUNCATE vs DELETE — are delegated to {@link
 * SqlDialectTestSupport}. Only packaged in the WAR when a JPA store profile (mysql, postgresql,
 * oracle) is active.
 */
@ApplicationScoped
public class JpaTestCleanupStrategy implements TestCleanupStrategy {

  private static final Logger log = Logger.getLogger(JpaTestCleanupStrategy.class.getName());

  private static final List<String> TABLES_BEFORE_HOT_STATE =
      List.of(
          "scheduler_workflow_condition",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_permit",
          "scheduler_job_tag",
          "scheduler_batch_metrics",
          "scheduler_batch",
          "scheduler_job_archive");

  private static final List<String> TABLES_AFTER_HOT_STATE =
      List.of(
          "scheduler_business_key_reservation",
          "scheduler_job_queue",
          "scheduler_job",
          "scheduler_recurring_job_archive",
          "scheduler_recurring_job",
          "scheduler_lock",
          "scheduler_resource_limit",
          "scheduler_node");

  @Inject private RatchetEntityManagerProvider entityManagerProvider;

  private static List<String> tablesToClear() {
    List<String> tables = new ArrayList<>(TABLES_BEFORE_HOT_STATE);
    tables.addAll(TABLES_AFTER_HOT_STATE);
    return tables;
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void truncateAll() {
    SqlDialectTestSupport dialect = SqlDialectTestSupportProvider.get();
    try {
      dialect.disableForeignKeyChecks(em());
      for (String table : tablesToClear()) {
        dialect.clearTable(em(), table);
      }
    } finally {
      try {
        dialect.enableForeignKeyChecks(em());
      } catch (Exception e) {
        log.fine("Unable to re-enable foreign key checks: " + e.getMessage());
      }
    }
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void deleteSchedulerLock(String name) {
    em().createNativeQuery("DELETE FROM scheduler_lock WHERE lock_name = ?")
        .setParameter(1, Objects.requireNonNull(name, "name"))
        .executeUpdate();
  }

  private EntityManager em() {
    return entityManagerProvider.getEntityManager();
  }
}
