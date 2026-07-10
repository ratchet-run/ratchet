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
package run.ratchet.store.sqlserver;

import jakarta.persistence.EntityManager;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JpaContainerFixture;

/** Shared Testcontainers + Hibernate fixture for SQL Server TCK tests. */
public class SqlserverTestFixture extends JpaContainerFixture {

  @SuppressWarnings("resource")
  private static final MSSQLServerContainer CONTAINER = MssqlContainers.shared();

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
    executeNativeSql("DELETE FROM scheduler_job_properties");
    executeNativeSql("DELETE FROM scheduler_job_extension_state");
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
    // opt-in Hibernate tuning and no-op under any other JPA provider.
    //
    // preferred_uuid_jdbc_type=BINARY: UUID columns are BINARY(16) holding canonical big-endian
    // bytes (matching the RI's native UuidByteArrayConverter.toBytes writes). Hibernate's SQL
    // Server
    // dialect otherwise defaults UUID to the uniqueidentifier JDBC type, so the driver writes
    // .NET-Guid mixed-endian bytes into BINARY(16) — JPA-managed writes (em.persist) and reads
    // (em.find) would then disagree with the native path, breaking FK joins and lookups. Forcing
    // the
    // BINARY type routes UUIDs through Hibernate's canonical UUIDJavaType byte order.
    return Map.of(
        "hibernate.hbm2ddl.auto", "none",
        "hibernate.show_sql", "false",
        "hibernate.format_sql", "false",
        "hibernate.connection.provider_disables_autocommit", "false",
        "hibernate.type.preferred_uuid_jdbc_type", "BINARY");
  }

  @Override
  protected String persistenceUnitName() {
    return "ratchet-sqlserver-tck";
  }

  @Override
  protected JobStore createStore(EntityManager em, MetricsCollector metrics) {
    SqlserverJobStoreImpl store =
        new SqlserverJobStoreImpl(() -> em, metrics, RatchetOptions.defaults());
    store.checkIsolationLevel();
    return store;
  }
}
