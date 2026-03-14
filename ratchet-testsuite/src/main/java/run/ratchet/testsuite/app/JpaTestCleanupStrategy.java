package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.UserTransaction;
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

  @PersistenceContext private EntityManager em;

  @Inject private UserTransaction utx;

  @Override
  public void truncateAll() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");

    try {
      utx.begin();
      try {
        if ("mysql".equals(dbType)) {
          em.createNativeQuery("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();
        }

        for (String table : TABLES_TO_TRUNCATE) {
          try {
            if ("postgresql".equals(dbType)) {
              em.createNativeQuery("TRUNCATE TABLE " + table + " CASCADE").executeUpdate();
            } else {
              em.createNativeQuery("TRUNCATE TABLE " + table).executeUpdate();
            }
          } catch (Exception e) {
            log.fine("Truncate skipped for " + table + ": " + e.getMessage());
          }
        }

        if ("mysql".equals(dbType)) {
          em.createNativeQuery("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();
        }

        utx.commit();
      } catch (Exception e) {
        utx.rollback();
        throw e;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to truncate tables", e);
    }
  }
}
