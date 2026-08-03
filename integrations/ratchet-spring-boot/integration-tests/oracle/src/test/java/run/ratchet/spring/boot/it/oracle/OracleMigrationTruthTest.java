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
package run.ratchet.spring.boot.it.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import run.ratchet.spring.boot.it.oracle.fixture.ratchetonly.RatchetOnlyApplication;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.oracle.OracleJobStore;
import run.ratchet.store.oracle.OracleSchemaMigrationDialect;

class OracleMigrationTruthTest extends OracleIntegrationTestSupport {

  @Test
  void emptyDatabaseWithAutoMigrateEnabledAppliesEveryDiscoveredMigration() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions("oracle"))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              SchemaMigrator migrator =
                  new SchemaMigrator(
                      context.getBean(DataSource.class),
                      context.getBean(OracleSchemaMigrationDialect.class));
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
              assertThat(context).hasSingleBean(OracleJobStore.class);
              assertThat(tableExists("ratchet_schema_version")).isFalse();
              assertThat(tableExists("scheduler_job")).isFalse();
            });
  }

  @Test
  void blankDialectInfersOracle() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions("   "))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(OracleSchemaMigrationDialect.class);
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
                      "requires ratchet.schema.migration-dialect to be blank or 'oracle',"
                          + " but was");
            });
  }

  @Test
  void migrationFailureFailsStartup() {
    contextRunner(MigrationFailureApplication.class, migrationOptions("oracle"))
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains("Ratchet Oracle schema auto-migration failed")
                  .contains("Intentional Oracle migration failure");
            });
  }

  @Test
  void migrationCompletesAndStoreInitializesWithoutAnIsolationProbe() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(OracleJobStore.class);
              assertThat(context).hasBean("oracleSchemaMigrationInitializer");

              SqlStatementProbe probe = context.getBean(SqlStatementProbe.class);
              SchemaMigrator migrator =
                  new SchemaMigrator(
                      context.getBean(DataSource.class),
                      context.getBean(OracleSchemaMigrationDialect.class));
              int expectedVersions = migrator.discoverMigrations().size();
              int migrationCompletion =
                  probe.lastIndexContaining("MERGE INTO ratchet_schema_version");
              assertThat(probe.countContaining("MERGE INTO ratchet_schema_version"))
                  .isEqualTo(expectedVersions);
              assertThat(migrationCompletion).isGreaterThanOrEqualTo(0);
              assertThat(probe.statements())
                  .noneMatch(
                      statement ->
                          statement.startsWith("select") && statement.contains("isolation"));
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
      return new SqlStatementProbe(sql -> sql.contains("create table ratchet_schema_lock"));
    }

    @Bean
    DataSource dataSource(SqlStatementProbe probe) {
      return probe.wrap(SqlProbeConfiguration.driverManagerDataSource());
    }
  }
}
