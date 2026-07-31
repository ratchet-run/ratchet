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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.migration.SchemaInitializationException;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;

/**
 * Applies PostgreSQL schema migrations before the job-store bean is created.
 *
 * <p>This object is an explicit dependency of the store bean. It deliberately invokes {@link
 * SchemaMigrator} directly, outside Spring transaction advice, because the migrator owns its JDBC
 * connection's commit, rollback, and auto-commit lifecycle.
 */
final class RatchetJpaSchemaMigrationInitializer {

  private static final String POSTGRESQL_DIALECT = "postgresql";

  RatchetJpaSchemaMigrationInitializer(
      ConfigurableListableBeanFactory beanFactory,
      RatchetOptions options,
      PostgresqlSchemaMigrationDialect dialect) {
    Objects.requireNonNull(beanFactory, "beanFactory");
    Objects.requireNonNull(options, "options");
    Objects.requireNonNull(dialect, "dialect");
    initialize(beanFactory, options.schema(), dialect);
  }

  private static void initialize(
      ConfigurableListableBeanFactory beanFactory,
      RatchetOptions.SchemaOptions schemaOptions,
      PostgresqlSchemaMigrationDialect dialect) {
    if (!schemaOptions.autoMigrate()) {
      return;
    }

    String configuredDialect = schemaOptions.migrationDialect();
    if (configuredDialect != null
        && !configuredDialect.isBlank()
        && !POSTGRESQL_DIALECT.equals(configuredDialect.trim().toLowerCase(Locale.ROOT))) {
      throw new SchemaInitializationException(
          "Ratchet PostgreSQL store requires ratchet.schema.migration-dialect to be blank or"
              + " 'postgresql', but was '"
              + configuredDialect
              + "'. Remove the conflicting dialect or use the matching SQL store.");
    }

    String[] dataSourceNames = beanFactory.getBeanNamesForType(DataSource.class, true, true);
    if (dataSourceNames.length != 1) {
      throw new SchemaInitializationException(
          "ratchet.schema.auto-migrate=true requires exactly one DataSource bean for the Ratchet"
              + " PostgreSQL store, but found "
              + dataSourceNames.length
              + ": "
              + Arrays.stream(dataSourceNames).sorted().toList()
              + ". Configure one application DataSource or set"
              + " ratchet.schema.auto-migrate=false.");
    }

    DataSource dataSource = beanFactory.getBean(dataSourceNames[0], DataSource.class);
    try {
      new SchemaMigrator(dataSource, dialect, schemaOptions.migrationPrefix()).migrate();
    } catch (IOException | SQLException | RuntimeException e) {
      throw new SchemaInitializationException(
          "Ratchet PostgreSQL schema auto-migration failed: " + exceptionSummary(e), e);
    }
  }

  private static String exceptionSummary(Exception exception) {
    String message = exception.getMessage();
    if (message == null || message.isBlank()) {
      return exception.getClass().getSimpleName();
    }
    return exception.getClass().getSimpleName() + ": " + message;
  }
}
