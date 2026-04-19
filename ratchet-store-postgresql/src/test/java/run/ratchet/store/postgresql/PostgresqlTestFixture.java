package run.ratchet.store.postgresql;

import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JpaContainerFixture;
import jakarta.persistence.EntityManager;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Shared Testcontainers + Hibernate fixture for PostgreSQL TCK tests.
 *
 * <p>Appends {@code ?stringtype=unspecified} to the JDBC URL so String-to-JSONB casts work without
 * a per-column type hint, mirroring the precedent established in {@code JdbcContainerExtension}.
 */
public class PostgresqlTestFixture extends JpaContainerFixture {

  @SuppressWarnings("resource")
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_test")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("stringtype", "unspecified")
          .withInitScript("ddl/postgresql-schema.sql")
          .withReuse(true);

  static {
    CONTAINER.start();
  }

  @Override
  protected JdbcDatabaseContainer<?> container() {
    return CONTAINER;
  }

  @Override
  protected Map<String, Object> jpaProperties() {
    // No hibernate.dialect pin — Hibernate 6 auto-detects from the JDBC URL. Remaining keys are
    // opt-in Hibernate tuning and no-op under any other JPA provider.
    return Map.of(
        "hibernate.hbm2ddl.auto", "none",
        "hibernate.show_sql", "false",
        "hibernate.format_sql", "false",
        "hibernate.connection.provider_disables_autocommit", "false");
  }

  @Override
  protected String persistenceUnitName() {
    return "ratchet-postgresql-tck";
  }

  @Override
  protected JobStore createStore(EntityManager em, MetricsCollector metrics) {
    return new PostgresqlJobStore(em);
  }

  @Override
  public boolean isStaleWriteException(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof RatchetOptimisticLockException) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void cleanupStore() {
    executeNativeSql("DELETE FROM scheduler_job_tag");
    executeNativeSql("DELETE FROM scheduler_job_log");
    executeNativeSql("DELETE FROM scheduler_job_execution");
    executeNativeSql("DELETE FROM scheduler_resource_permit");
    executeNativeSql("DELETE FROM scheduler_workflow_condition");
    executeNativeSql("DELETE FROM scheduler_dlq_alerts");
    executeNativeSql("DELETE FROM scheduler_batch_metrics");
    executeNativeSql("DELETE FROM scheduler_job_archive");
    executeNativeSql("DELETE FROM scheduler_job");
    executeNativeSql("DELETE FROM scheduler_batch");
    executeNativeSql("DELETE FROM scheduler_resource_limit");
    executeNativeSql("DELETE FROM scheduler_lock");
    executeNativeSql("DELETE FROM scheduler_node");
  }
}
