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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import example.ratchet.ConsumerApplication;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.migration.SchemaMigrator;

/**
 * Exercises migration and validation through the same datasource and vendor factory as a consumer.
 */
class SqlSchemaLifecycleTest {
  @Test
  void freshConsumerMigratesTheCurrentRatchetSchema() {
    try (SqlDatabase database = SqlDatabase.start();
        ConfigurableApplicationContext context = application(database)) {
      JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);

      assertThat(jdbc.queryForObject("select count(*) from ratchet_schema_version", Integer.class))
          .isGreaterThan(0);
      assertThat(jdbc.queryForObject("select count(*) from scheduler_job", Integer.class)).isZero();
    }
  }

  @Test
  void externallyManagedCurrentSchemaValidatesWithoutCreatingMigrationMetadata() throws Exception {
    try (SqlDatabase database = SqlDatabase.start()) {
      try (ConfigurableApplicationContext migrated = application(database)) {
        migrated.getBean(JdbcTemplate.class).execute("drop table ratchet_schema_version");
      }
      assertThat(hasTable(database, "ratchet_schema_version")).isFalse();

      try (ConfigurableApplicationContext validated =
          application(database, "ratchet.schema.auto-migrate=false")) {
        assertThat(
                validated
                    .getBean(JdbcTemplate.class)
                    .queryForObject("select count(*) from scheduler_job", Integer.class))
            .isZero();
      }
      assertThat(hasTable(database, "ratchet_schema_version")).isFalse();
    }
  }

  @Test
  void validationOnlyModeRejectsAnIncompleteExistingMigrationLedger() {
    try (SqlDatabase database = SqlDatabase.start()) {
      try (ConfigurableApplicationContext migrated = application(database)) {
        JdbcTemplate jdbc = migrated.getBean(JdbcTemplate.class);
        String firstVersion =
            jdbc.queryForObject("select min(version) from ratchet_schema_version", String.class);
        jdbc.update("delete from ratchet_schema_version where version <> ?", firstVersion);
      }

      assertThatThrownBy(() -> application(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("missing recorded migration");
    }
  }

  @Test
  void validationOnlyModeRejectsAnEmptySchemaWithoutCreatingMigrationMetadata() {
    try (SqlDatabase database = SqlDatabase.start()) {
      assertThatThrownBy(() -> application(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("Ratchet schema is missing required table");

      assertThat(hasTable(database, "ratchet_schema_version")).isFalse();
    } catch (Exception e) {
      throw new AssertionError("Could not inspect the validation-only schema", e);
    }
  }

  @Test
  void validationOnlyModeRejectsAMissingIdempotencyUniqueIndex() {
    try (SqlDatabase database = SqlDatabase.start()) {
      try (ConfigurableApplicationContext migrated = application(database)) {
        dropIdempotencyUniqueIndex(database, migrated.getBean(JdbcTemplate.class));
      }

      assertThatThrownBy(() -> application(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("uk_idempotency_key");
    }
  }

  @Test
  void validationOnlyModeRejectsMissingPrincipalEvenWhenSiblingMatchesMetadataPattern() {
    try (SqlDatabase database = SqlDatabase.start()) {
      try (ConfigurableApplicationContext migrated = application(database)) {
        JdbcTemplate jdbc = migrated.getBean(JdbcTemplate.class);
        hideRequiredPrincipalColumnAndCreateSibling(database, jdbc);
      }

      assertThatThrownBy(() -> migrator(database).validate())
          .hasStackTraceContaining("scheduler_job")
          .hasStackTraceContaining("caller_principal");
    }
  }

  @Test
  void migrationFailureDoesNotPreventAFollowingConsumerFromStarting() {
    try (SqlDatabase database = SqlDatabase.start()) {
      assertThatThrownBy(
              () ->
                  application(
                      database, "ratchet.schema.migration-prefix=missing-ratchet-migrations"))
          .hasStackTraceContaining("No Ratchet schema migration scripts were discovered");

      try (ConfigurableApplicationContext recovered = application(database)) {
        assertThat(
                recovered
                    .getBean(JdbcTemplate.class)
                    .queryForObject("select count(*) from ratchet_schema_version", Integer.class))
            .isGreaterThan(0);
      }
    }
  }

  @Test
  void concurrentMigratorsApplyEachMigrationOnce() throws Exception {
    try (SqlDatabase database = SqlDatabase.start()) {
      SchemaMigrator probe = migrator(database);
      int expectedMigrations = probe.discoverMigrations().size();
      CyclicBarrier ready = new CyclicBarrier(2);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        var first = executor.submit(() -> migrateAfter(ready, database));
        var second = executor.submit(() -> migrateAfter(ready, database));
        SchemaMigrator.MigrationResult firstResult = first.get(2, TimeUnit.MINUTES);
        SchemaMigrator.MigrationResult secondResult = second.get(2, TimeUnit.MINUTES);

        assertThat(firstResult.appliedCount() + secondResult.appliedCount())
            .isEqualTo(expectedMigrations);
        assertThat(firstResult.skippedCount() + secondResult.skippedCount())
            .isEqualTo(expectedMigrations);
      } finally {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  private static ConfigurableApplicationContext application(
      SqlDatabase database, String... properties) {
    SpringApplicationBuilder builder =
        new SpringApplicationBuilder(ConsumerApplication.class).properties(database.properties());
    for (String property : properties) {
      builder.properties(property);
    }
    return builder.run();
  }

  private static SchemaMigrator.MigrationResult migrateAfter(
      CyclicBarrier ready, SqlDatabase database) throws Exception {
    ready.await(30, TimeUnit.SECONDS);
    return migrator(database).migrate();
  }

  private static SchemaMigrator migrator(SqlDatabase database) {
    var dataSource = dataSource(database);
    return new SchemaMigrator(dataSource, SqlStoreVendor.detect(dataSource).migrationDialect());
  }

  private static DriverManagerDataSource dataSource(SqlDatabase database) {
    Map<String, Object> properties = database.properties();
    return new DriverManagerDataSource(
        (String) properties.get("spring.datasource.url"),
        (String) properties.get("spring.datasource.username"),
        (String) properties.get("spring.datasource.password"));
  }

  private static boolean hasTable(SqlDatabase database, String tableName) throws Exception {
    try (var connection = dataSource(database).getConnection();
        var tables =
            connection
                .getMetaData()
                .getTables(connection.getCatalog(), connection.getSchema(), "%", null)) {
      while (tables.next()) {
        if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) return true;
      }
      return false;
    }
  }

  private static void dropIdempotencyUniqueIndex(SqlDatabase database, JdbcTemplate jdbc) {
    switch (database.store()) {
      case "mysql" -> jdbc.execute("alter table scheduler_job drop index uk_idempotency_key");
      case "postgresql", "oracle", "sqlserver" ->
          jdbc.execute("alter table scheduler_job drop constraint uk_idempotency_key");
      default ->
          throw new IllegalArgumentException("Unsupported SQL consumer store: " + database.store());
    }
  }

  private static void hideRequiredPrincipalColumnAndCreateSibling(
      SqlDatabase database, JdbcTemplate jdbc) {
    // Use a column without generated-column dependencies so this fixture works on every vendor.
    switch (database.store()) {
      case "mysql", "oracle", "postgresql" ->
          jdbc.execute(
              "alter table scheduler_job rename column caller_principal to principal_missing");
      case "sqlserver" ->
          jdbc.execute(
              "exec sp_rename 'scheduler_job.caller_principal', 'principal_missing', 'COLUMN'");
      default ->
          throw new IllegalArgumentException("Unsupported SQL consumer store: " + database.store());
    }
    jdbc.execute("create table schedulerXjob (caller_principal varchar(32))");
  }
}
