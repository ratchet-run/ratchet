package run.ratchet.testsuite.app;

import run.ratchet.store.spi.RatchetEntityManagerProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.util.List;
import java.util.logging.Logger;

/**
 * JPA/SQL implementation of {@link TestCleanupStrategy}.
 *
 * <p>Truncates all scheduler tables using native SQL within a JTA transaction. Foreign key checks
 * are temporarily disabled for MySQL. Only packaged in the WAR when a JPA store profile (mysql,
 * postgresql) is active.
 */
@ApplicationScoped
public class JpaTestCleanupStrategy implements TestCleanupStrategy {

  private static final Logger log = Logger.getLogger(JpaTestCleanupStrategy.class.getName());

  private static final List<String> TABLES_TO_TRUNCATE =
      List.of(
          "scheduler_workflow_condition",
          "scheduler_dlq_alerts",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_permit",
          "scheduler_job_tag",
          "scheduler_batch_metrics",
          "scheduler_batch",
          "scheduler_job_archive",
          "scheduler_job",
          "scheduler_lock",
          "scheduler_resource_limit",
          "scheduler_node");

  @Inject private RatchetEntityManagerProvider entityManagerProvider;

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void truncateAll() {
    String dbType = TestRuntimeConfig.dbType();

    try {
      if ("mysql".equals(dbType)) {
        em().createNativeQuery("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();
      }

      for (String table : TABLES_TO_TRUNCATE) {
        try {
          if ("postgresql".equals(dbType)) {
            em().createNativeQuery("TRUNCATE TABLE " + table + " CASCADE").executeUpdate();
          } else {
            em().createNativeQuery("TRUNCATE TABLE " + table).executeUpdate();
          }
        } catch (Exception e) {
          log.fine("Truncate skipped for " + table + ": " + e.getMessage());
        }
      }
    } finally {
      if ("mysql".equals(dbType)) {
        try {
          em().createNativeQuery("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();
        } catch (Exception e) {
          log.fine("Unable to re-enable MySQL foreign key checks: " + e.getMessage());
        }
      }
    }
  }

  private EntityManager em() {
    return entityManagerProvider.getEntityManager();
  }
}
