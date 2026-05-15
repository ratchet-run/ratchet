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

/**
 * JPA/SQL implementation of {@link TestCleanupStrategy}.
 *
 * <p>Clears all scheduler tables using native SQL within a JTA transaction. Foreign key checks are
 * temporarily disabled for MySQL. Only packaged in the WAR when a JPA store profile (mysql,
 * postgresql) is active.
 */
@ApplicationScoped
public class JpaTestCleanupStrategy implements TestCleanupStrategy {

  private static final Logger log = Logger.getLogger(JpaTestCleanupStrategy.class.getName());
  private static final String DB_TYPE_MYSQL = "mysql";
  private static final String DB_TYPE_POSTGRESQL = "postgresql";

  private static final List<String> TABLES_BEFORE_HOT_STATE =
      List.of(
          "scheduler_workflow_condition",
          "scheduler_dlq_alerts",
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
    String dbType = TestRuntimeConfig.dbType();
    boolean mysql = DB_TYPE_MYSQL.equals(dbType);
    boolean postgresql = DB_TYPE_POSTGRESQL.equals(dbType);

    try {
      if (mysql) {
        em().createNativeQuery("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();
      }

      for (String table : tablesToClear()) {
        if (postgresql) {
          em().createNativeQuery("DELETE FROM " + table).executeUpdate();
        } else {
          em().createNativeQuery("TRUNCATE TABLE " + table).executeUpdate();
        }
      }
    } finally {
      if (mysql) {
        try {
          em().createNativeQuery("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();
        } catch (Exception e) {
          log.fine("Unable to re-enable MySQL foreign key checks: " + e.getMessage());
        }
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
