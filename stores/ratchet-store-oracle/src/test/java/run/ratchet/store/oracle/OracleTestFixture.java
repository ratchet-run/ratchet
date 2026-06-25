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
package run.ratchet.store.oracle;

import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JpaContainerFixture;

/** Shared Testcontainers + Hibernate fixture for Oracle TCK tests. */
public class OracleTestFixture extends JpaContainerFixture {

  @SuppressWarnings("resource")
  private static final OracleContainer CONTAINER =
      new OracleContainer("gvenzl/oracle-free:slim-faststart")
          .withDatabaseName("ratchet_test")
          .withUsername("ratchet")
          .withPassword("ratchet")
          // Oracle's SGA needs far more than Docker's default 64 MB /dev/shm; without this the
          // instance OOMs while opening the database (ORA-03113 end-of-file on communication
          // channel). Round-tripped Instant correctness is handled by hibernate.jdbc.time_zone=UTC
          // (see jpaProperties) rather than vendor URL params.
          .withSharedMemorySize(2L * 1024 * 1024 * 1024)
          .withStartupTimeout(Duration.ofMinutes(5))
          .withInitScript("ddl/oracle-schema.sql")
          .withReuse(true);

  static {
    CONTAINER.start();
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
    executeNativeSql("DELETE FROM scheduler_recurring_job_archive");
    executeNativeSql("DELETE FROM scheduler_recurring_job");
    executeNativeSql("DELETE FROM scheduler_batch");
    executeNativeSql("DELETE FROM scheduler_resource_limit");
    executeNativeSql("DELETE FROM scheduler_lock");
    executeNativeSql("DELETE FROM scheduler_node");
  }

  @Override
  protected JdbcDatabaseContainer<?> container() {
    return CONTAINER;
  }

  @Override
  protected Map<String, Object> jpaProperties() {
    // No hibernate.dialect pin — Hibernate 6 auto-detects from the JDBC URL. Remaining keys are
    // opt-in Hibernate tuning and no-op under any other JPA provider. connection.isolation=2 maps
    // to READ_COMMITTED, Oracle's default and the isolation the claim path expects. jdbc.time_zone
    // = UTC makes Timestamp binding/reading interpret the plain TIMESTAMP columns as UTC regardless
    // of the JVM zone, so Instant round-trips match the UTC wall-clock the store writes.
    return Map.of(
        "hibernate.hbm2ddl.auto", "none",
        "hibernate.show_sql", "false",
        "hibernate.format_sql", "false",
        "hibernate.connection.provider_disables_autocommit", "false",
        "hibernate.jdbc.time_zone", "UTC",
        // Map entity Instant fields to plain TIMESTAMP, not Hibernate 6's default TIMESTAMP_UTC
        // (Oracle TIMESTAMP WITH TIME ZONE). The schema columns are plain TIMESTAMP(6); without
        // this
        // Hibernate reads them as OffsetDateTime and Oracle raises ORA-18716 "not in any time
        // zone".
        "hibernate.type.preferred_instant_jdbc_type", "TIMESTAMP",
        "hibernate.connection.isolation", "2");
  }

  @Override
  protected String persistenceUnitName() {
    return "ratchet-oracle-tck";
  }

  @Override
  protected JobStore createStore(EntityManager em, MetricsCollector metrics) {
    OracleJobStoreImpl store = new OracleJobStoreImpl(() -> em, metrics, RatchetOptions.defaults());
    store.checkIsolationLevel();
    return store;
  }
}
