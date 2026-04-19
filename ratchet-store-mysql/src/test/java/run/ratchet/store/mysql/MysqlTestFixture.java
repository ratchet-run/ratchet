package run.ratchet.store.mysql;

import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JpaContainerFixture;
import jakarta.persistence.EntityManager;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mysql.MySQLContainer;

/** Shared Testcontainers + Hibernate fixture for MySQL TCK tests. */
public class MysqlTestFixture extends JpaContainerFixture {

  @SuppressWarnings("resource")
  private static final MySQLContainer CONTAINER =
      new MySQLContainer("mysql:8.0")
          .withDatabaseName("ratchet_test")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withInitScript("ddl/mysql-schema.sql")
          .withReuse(true);

  static {
    CONTAINER.start();
  }

  @Override
  protected JdbcDatabaseContainer<?> container() {
    return CONTAINER;
  }

  @Override
  protected String hibernateDialect() {
    return "org.hibernate.dialect.MySQLDialect";
  }

  @Override
  protected String persistenceUnitName() {
    return "ratchet-mysql-tck";
  }

  @Override
  protected JobStore createStore(EntityManager em, MetricsCollector metrics) {
    return new MysqlJobStore(em, metrics);
  }

  @Override
  public void cleanupStore() {
    // Order matters: delete children before parents, bkres before queue, queue before job.
    executeNativeSql("DELETE FROM scheduler_business_key_reservation");
    executeNativeSql("DELETE FROM scheduler_job_queue");
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
