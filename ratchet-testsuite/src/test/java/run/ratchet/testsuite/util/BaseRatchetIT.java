package run.ratchet.testsuite.util;

import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.UserTransaction;
import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract base class for Ratchet integration tests.
 *
 * <p>Provides Arquillian lifecycle management via {@link ArquillianExtension}. Database container
 * management is handled by DatabaseContainerExtension, which is registered via JUnit 5 automatic
 * extension detection (see {@code META-INF/services/org.junit.jupiter.api.extension.Extension}).
 * This separation ensures the Testcontainers-dependent extension is only loaded on the client side,
 * not inside the WildFly container.
 *
 * <p>Each test class starts with a clean database via {@link #truncateAll()}.
 */
@ExtendWith(ArquillianExtension.class)
public abstract class BaseRatchetIT {

  private static final Logger log = Logger.getLogger(BaseRatchetIT.class.getName());

  /** Tables to truncate before each test class, in dependency order (children first). */
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

  @PersistenceContext protected EntityManager em;

  @Inject protected UserTransaction utx;

  /**
   * Truncates all scheduler tables before each test to ensure test isolation.
   *
   * <p>Uses direct SQL to avoid JPA cache issues. Foreign key checks are temporarily disabled for
   * MySQL to allow truncation in any order.
   */
  @BeforeEach
  protected void truncateAll() throws Exception {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");

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
  }
}
