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
package run.ratchet.spring.boot.it.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import run.ratchet.spring.boot.it.sqlserver.fixture.ratchetonly.RatchetOnlyApplication;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.sqlserver.SqlserverJobStore;
import run.ratchet.store.sqlserver.SqlserverSchemaMigrationDialect;

class SqlserverMigrationTruthTest extends SqlserverIntegrationTestSupport {

  @Test
  void emptyDatabaseWithAutoMigrateEnabledAppliesEveryDiscoveredMigration() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions("sqlserver"))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              SchemaMigrator migrator =
                  new SchemaMigrator(
                      context.getBean(DataSource.class),
                      context.getBean(SqlserverSchemaMigrationDialect.class));
              int expectedVersions = migrator.discoverMigrations().size();

              assertThat(expectedVersions).isPositive();
              assertThat(queryForLong("SELECT COUNT(*) FROM ratchet_schema_version"))
                  .isEqualTo(expectedVersions);
              assertThat(tableExists("scheduler_job")).isTrue();
            });
  }

  @Test
  void autoMigrateDisabledDoesNotCreateAnySchemaObjects() {
    contextRunner(RatchetOnlyApplication.class, noMigrationOptions())
        .withPropertyValues("ratchet.lifecycle.defer-auto-start=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(SqlserverJobStore.class);
              assertThat(tableExists("ratchet_schema_version")).isFalse();
              assertThat(tableExists("scheduler_job")).isFalse();
            });
  }

  @Test
  void blankDialectInfersSqlserver() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions("   "))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(SqlserverSchemaMigrationDialect.class);
              assertThat(queryForLong("SELECT COUNT(*) FROM ratchet_schema_version")).isPositive();
            });
  }

  @Test
  void explicitConflictingDialectFailsStartup() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions("postgresql"))
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains(
                      "requires ratchet.schema.migration-dialect to be blank or 'sqlserver',"
                          + " but was");
            });
  }

  @Test
  void migrationFailureFailsStartup() {
    contextRunner(MigrationFailureApplication.class, migrationOptions("sqlserver"))
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains("Ratchet SQL Server schema auto-migration failed")
                  .contains("Intentional SQL Server migration failure");
            });
  }

  @Test
  void migrationCompletesBeforeStoreInitializationQueriesIsolation() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(SqlserverJobStore.class);
              assertThat(context).hasBean("sqlserverSchemaMigrationInitializer");

              SqlStatementProbe probe = context.getBean(SqlStatementProbe.class);
              SchemaMigrator migrator =
                  new SchemaMigrator(
                      context.getBean(DataSource.class),
                      context.getBean(SqlserverSchemaMigrationDialect.class));
              int expectedVersions = migrator.discoverMigrations().size();
              int migrationCompletion = probe.lastIndexContaining("merge ratchet_schema_version");
              List<String> statements = probe.statements();
              int storeInitialization =
                  IntStream.range(0, statements.size())
                      .filter(
                          index -> {
                            String statement = statements.get(index);
                            return statement.startsWith("select")
                                && statement.contains("sys.dm_exec_sessions");
                          })
                      .findFirst()
                      .orElse(-1);
              assertThat(probe.countContaining("merge ratchet_schema_version"))
                  .isEqualTo(expectedVersions);
              assertThat(migrationCompletion).isGreaterThanOrEqualTo(0);
              assertThat(storeInitialization).isGreaterThan(migrationCompletion);
              assertThat(statements)
                  .filteredOn(
                      statement ->
                          statement.startsWith("select")
                              && statement.contains("sys.dm_exec_sessions"))
                  .hasSize(1);
            });
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  @Import(FailingMigrationDataSourceConfiguration.class)
  static class MigrationFailureApplication {}

  @Configuration(proxyBeanMethods = false)
  static class FailingMigrationDataSourceConfiguration {

    @Bean
    SqlStatementProbe sqlStatementProbe() {
      return new SqlStatementProbe(sql -> sql.contains("sp_getapplock"));
    }

    @Bean
    DataSource dataSource(SqlStatementProbe probe) {
      return probe.wrap(SqlProbeConfiguration.driverManagerDataSource());
    }
  }
}
