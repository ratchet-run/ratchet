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
  POSTGRESQL,
  MYSQL,
  ORACLE,
  SQLSERVER;

  static SqlStoreVendor detect(DataSource dataSource) {
    try (var connection = dataSource.getConnection()) {
      String product = connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
      if (product.contains("postgres")) return POSTGRESQL;
      if (product.contains("mysql") || product.contains("mariadb")) return MYSQL;
      if (product.contains("oracle")) return ORACLE;
      if (product.contains("sql server") || product.contains("microsoft")) return SQLSERVER;
      throw new IllegalStateException(
          "Ratchet does not support database product '" + product + "'");
    } catch (SQLException e) {
      throw new IllegalStateException("Ratchet could not inspect the configured DataSource", e);
    }
  }

  Class<? extends JobStore> storeType() {
    return requireVendor(
        "store interface",
        () ->
            switch (this) {
              case POSTGRESQL -> PostgresqlVendor.storeType();
              case MYSQL -> MysqlVendor.storeType();
              case ORACLE -> OracleVendor.storeType();
              case SQLSERVER -> SqlserverVendor.storeType();
            });
  }

  SchemaMigrationDialect migrationDialect() {
    return requireVendor(
        "schema migration dialect",
        () ->
            switch (this) {
              case POSTGRESQL -> PostgresqlVendor.migrationDialect();
              case MYSQL -> MysqlVendor.migrationDialect();
              case ORACLE -> OracleVendor.migrationDialect();
              case SQLSERVER -> SqlserverVendor.migrationDialect();
            });
  }

  JobStore createStore(
      EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
    return requireVendor(
        "JobStore",
        () ->
            switch (this) {
              case POSTGRESQL -> PostgresqlVendor.createStore(entityManager, options, metrics);
              case MYSQL -> MysqlVendor.createStore(entityManager, options, metrics);
              case ORACLE -> OracleVendor.createStore(entityManager, options, metrics);
              case SQLSERVER -> SqlserverVendor.createStore(entityManager, options, metrics);
            });
  }

  String jpaMappingFile() {
    return switch (this) {
      case POSTGRESQL -> "META-INF/orm-postgresql.xml";
      case MYSQL -> "META-INF/orm-mysql.xml";
      case ORACLE -> "META-INF/orm-oracle.xml";
      case SQLSERVER -> "META-INF/orm-sqlserver.xml";
    };
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
    return switch (this) {
      case POSTGRESQL -> "run.ratchet:ratchet-store-postgresql";
      case MYSQL -> "run.ratchet:ratchet-store-mysql";
      case ORACLE -> "run.ratchet:ratchet-store-oracle";
      case SQLSERVER -> "run.ratchet:ratchet-store-sqlserver";
    };
  }

  private static final class PostgresqlVendor {
    private static Class<? extends JobStore> storeType() {
      return PostgresqlJobStore.class;
    }

    private static SchemaMigrationDialect migrationDialect() {
      return new PostgresqlSchemaMigrationDialect();
    }

    private static JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return PostgresqlJobStoreFactory.create(entityManager, options, metrics);
    }
  }

  private static final class MysqlVendor {
    private static Class<? extends JobStore> storeType() {
      return MysqlJobStore.class;
    }

    private static SchemaMigrationDialect migrationDialect() {
      return new MysqlSchemaMigrationDialect();
    }

    private static JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return MysqlJobStoreFactory.create(entityManager, options, metrics);
    }
  }

  private static final class OracleVendor {
    private static Class<? extends JobStore> storeType() {
      return OracleJobStore.class;
    }

    private static SchemaMigrationDialect migrationDialect() {
      return new OracleSchemaMigrationDialect();
    }

    private static JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return OracleJobStoreFactory.create(entityManager, options, metrics);
    }
  }

  private static final class SqlserverVendor {
    private static Class<? extends JobStore> storeType() {
      return SqlserverJobStore.class;
    }

    private static SchemaMigrationDialect migrationDialect() {
      return new SqlserverSchemaMigrationDialect();
    }

    private static JobStore createStore(
        EntityManager entityManager, RatchetOptions options, MetricsCollector metrics) {
      return SqlserverJobStoreFactory.create(entityManager, options, metrics);
    }
  }
}
