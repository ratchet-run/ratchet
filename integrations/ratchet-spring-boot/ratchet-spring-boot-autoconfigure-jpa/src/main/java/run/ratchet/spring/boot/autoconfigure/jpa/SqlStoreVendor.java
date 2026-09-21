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
package run.ratchet.spring.boot.autoconfigure.jpa;

import jakarta.persistence.EntityManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.springframework.util.ClassUtils;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.mysql.MysqlJobStore;
import run.ratchet.store.mysql.MysqlJobStoreFactory;
import run.ratchet.store.mysql.MysqlSchemaMigrationDialect;
import run.ratchet.store.oracle.OracleJobStore;
import run.ratchet.store.oracle.OracleJobStoreFactory;
import run.ratchet.store.oracle.OracleSchemaMigrationDialect;
import run.ratchet.store.postgresql.PostgresqlJobStore;
import run.ratchet.store.postgresql.PostgresqlJobStoreFactory;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.sqlserver.SqlserverJobStore;
import run.ratchet.store.sqlserver.SqlserverJobStoreFactory;
import run.ratchet.store.sqlserver.SqlserverSchemaMigrationDialect;

/** Supported SQL stores selected from the application's JDBC metadata. */
enum SqlStoreVendor {
  POSTGRESQL("META-INF/orm-postgresql.xml", "run.ratchet:ratchet-store-postgresql"),
  MYSQL("META-INF/orm-mysql.xml", "run.ratchet:ratchet-store-mysql"),
  ORACLE("META-INF/orm-oracle.xml", "run.ratchet:ratchet-store-oracle"),
  SQLSERVER("META-INF/orm-sqlserver.xml", "run.ratchet:ratchet-store-sqlserver");

  private final String jpaMappingFile;
  private final String artifactId;

  SqlStoreVendor(String jpaMappingFile, String artifactId) {
    this.jpaMappingFile = jpaMappingFile;
    this.artifactId = artifactId;
  }

  static SqlStoreVendor detect(DataSource dataSource) {
    try (var connection = dataSource.getConnection()) {
      String product = connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
      if (product.contains("postgres")) return validateAotVendor(POSTGRESQL);
      if (product.contains("mysql") || product.contains("mariadb")) return validateAotVendor(MYSQL);
      if (product.contains("oracle")) return validateAotVendor(ORACLE);
      if (product.contains("sql server") || product.contains("microsoft"))
        return validateAotVendor(SQLSERVER);
      throw new IllegalStateException(
          "Ratchet does not support database product '" + product + "'");
    } catch (SQLException e) {
      throw new IllegalStateException("Ratchet could not inspect the configured DataSource", e);
    }
  }

  private static SqlStoreVendor validateAotVendor(SqlStoreVendor detected) {
    String expected = RatchetJpaAotSettings.get("vendor");
    if (expected != null && !detected.name().equals(expected)) {
      throw new IllegalStateException(
          "Ratchet executable was built for "
              + expected
              + " but the configured database is "
              + detected);
    }
    return detected;
  }

  String storeClassName() {
    String title =
        switch (this) {
          case POSTGRESQL -> "Postgresql";
          case MYSQL -> "Mysql";
          case ORACLE -> "Oracle";
          case SQLSERVER -> "Sqlserver";
        };
    return "run.ratchet.store." + name().toLowerCase(Locale.ROOT) + "." + title + "JobStore";
  }

  String adapterClassName() {
    return SqlStoreVendor.class.getName()
        + "$"
        + switch (this) {
          case POSTGRESQL -> "PostgresqlVendor";
          case MYSQL -> "MysqlVendor";
          case ORACLE -> "OracleVendor";
          case SQLSERVER -> "SqlserverVendor";
        };
  }

  private Vendor adapter() {
    // Only the installed store's adapter is registered by AOT. There are no static call edges
    // from this dispatcher to absent, optional store implementations in native-image analysis.
    try {
      return (Vendor)
          ClassUtils.forName(adapterClassName(), getClass().getClassLoader())
              .getDeclaredConstructor()
              .newInstance();
    } catch (ReflectiveOperationException | LinkageError failure) {
      throw new IllegalStateException("Ratchet requires " + artifactId, failure);
    }
  }

  Class<? extends JobStore> storeType() {
    return requireVendor("store interface", () -> adapter().storeType());
  }

  SchemaMigrationDialect migrationDialect() {
    return requireVendor("schema migration dialect", () -> adapter().migrationDialect());
  }

  JobStore createStore(
      EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
    return requireVendor("JobStore", () -> adapter().createStore(entityManager, options, metrics));
  }

  interface Vendor {
    Class<? extends JobStore> storeType();

    SchemaMigrationDialect migrationDialect();

    JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics);
  }

  String jpaMappingFile() {
    return jpaMappingFile;
  }

  private <T> T requireVendor(String capability, Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (LinkageError e) {
      throw new IllegalStateException(
          "Ratchet detected "
              + name()
              + " but its "
              + capability
              + " is unavailable. Add "
              + artifactId()
              + " to the application dependencies.",
          e);
    }
  }

  private String artifactId() {
    return artifactId;
  }

  static final class PostgresqlVendor implements Vendor {
    public Class<? extends JobStore> storeType() {
      return PostgresqlJobStore.class;
    }

    public SchemaMigrationDialect migrationDialect() {
      return new PostgresqlSchemaMigrationDialect();
    }

    public JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return PostgresqlJobStoreFactory.create(entityManager, options, metrics);
    }
  }

  static final class MysqlVendor implements Vendor {
    public Class<? extends JobStore> storeType() {
      return MysqlJobStore.class;
    }

    public SchemaMigrationDialect migrationDialect() {
      return new MysqlSchemaMigrationDialect();
    }

    public JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return MysqlJobStoreFactory.create(entityManager, options, metrics);
    }
  }

  static final class OracleVendor implements Vendor {
    public Class<? extends JobStore> storeType() {
      return OracleJobStore.class;
    }

    public SchemaMigrationDialect migrationDialect() {
      return new OracleSchemaMigrationDialect();
    }

    public JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return OracleJobStoreFactory.create(entityManager, options, metrics);
    }
  }

  static final class SqlserverVendor implements Vendor {
    public Class<? extends JobStore> storeType() {
      return SqlserverJobStore.class;
    }

    public SchemaMigrationDialect migrationDialect() {
      return new SqlserverSchemaMigrationDialect();
    }

    public JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return SqlserverJobStoreFactory.create(entityManager, options, metrics);
    }
  }
}
