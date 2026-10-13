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
package run.ratchet.consumer.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Black-box startup migration and validation; no integration implementation imports. */
class SqlSchemaLifecycleTest {
  @Test
  void freshConsumerMigratesCurrentSchema() {
    try (var database = SqlDatabase.start();
        var context = SqlApplication.start(database)) {
      var jdbc = new SqlAccess(context);
      assertThat(jdbc.queryForObject("select count(*) from ratchet_schema_version", Integer.class))
          .isGreaterThan(0);
      assertThat(jdbc.queryForObject("select count(*) from scheduler_job", Integer.class)).isZero();
    }
  }

  @Test
  void externalSchemaValidatesWithoutRecreatingMigrationLedger() throws Exception {
    try (var database = SqlDatabase.start()) {
      try (var migrated = SqlApplication.start(database)) {
        new SqlAccess(migrated).execute("drop table ratchet_schema_version");
      }
      assertThat(hasTable(database, "ratchet_schema_version")).isFalse();
      try (var validated = SqlApplication.start(database, "ratchet.schema.auto-migrate=false")) {
        assertThat(
                new SqlAccess(validated)
                    .queryForObject("select count(*) from scheduler_job", Integer.class))
            .isZero();
      }
      assertThat(hasTable(database, "ratchet_schema_version")).isFalse();
    }
  }

  @Test
  void validationOnlyRejectsEmptySchemaWithoutCreatingMetadata() throws Exception {
    try (var database = SqlDatabase.start()) {
      assertThatThrownBy(() -> SqlApplication.start(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("scheduler_job");
      assertThat(hasTable(database, "ratchet_schema_version")).isFalse();
      assertThat(hasTable(database, "scheduler_job")).isFalse();
    }
  }

  @Test
  void validationOnlyRejectsIncompleteMigrationLedger() {
    try (var database = SqlDatabase.start()) {
      try (var migrated = SqlApplication.start(database)) {
        var jdbc = new SqlAccess(migrated);
        String first =
            jdbc.queryForObject("select min(version) from ratchet_schema_version", String.class);
        jdbc.update("delete from ratchet_schema_version where version <> ?", first);
      }
      assertThatThrownBy(() -> SqlApplication.start(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("missing recorded migration");
    }
  }

  @Test
  void validationOnlyRejectsMissingIdempotencyUniqueIndex() {
    try (var database = SqlDatabase.start()) {
      try (var migrated = SqlApplication.start(database)) {
        var jdbc = new SqlAccess(migrated);
        if (database.store().equals("mysql"))
          jdbc.execute("alter table scheduler_job drop index uk_idempotency_key");
        else jdbc.execute("alter table scheduler_job drop constraint uk_idempotency_key");
      }
      assertThatThrownBy(() -> SqlApplication.start(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("uk_idempotency_key");
    }
  }

  @Test
  void validationUsesLiteralTableNamesAndDetectsMissingRequiredColumns() {
    try (var database = SqlDatabase.start()) {
      try (var migrated = SqlApplication.start(database)) {
        var jdbc = new SqlAccess(migrated);
        switch (database.store()) {
          case "mysql", "oracle", "postgresql" ->
              jdbc.execute(
                  "alter table scheduler_job rename column caller_principal to principal_missing");
          case "sqlserver" ->
              jdbc.execute(
                  "exec sp_rename 'scheduler_job.caller_principal', 'principal_missing', 'COLUMN'");
          default -> throw new IllegalArgumentException(database.store());
        }
        jdbc.execute("create table schedulerXjob (caller_principal varchar(32))");
      }
      assertThatThrownBy(() -> SqlApplication.start(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("scheduler_job")
          .hasStackTraceContaining("caller_principal");
    }
  }

  @Test
  void migrationFailureDoesNotPreventFollowingConsumerStartup() {
    try (var database = SqlDatabase.start()) {
      assertThatThrownBy(
              () ->
                  SqlApplication.start(
                      database, "ratchet.schema.migration-prefix=missing-ratchet-migrations"))
          .hasStackTraceContaining("No Ratchet schema migration scripts were discovered");
      try (var recovered = SqlApplication.start(database)) {
        assertThat(
                new SqlAccess(recovered)
                    .queryForObject("select count(*) from ratchet_schema_version", Integer.class))
            .isGreaterThan(0);
      }
    }
  }

  private static boolean hasTable(SqlDatabase database, String table) throws Exception {
    try (var connection = SqlApplication.connection(database);
        var tables =
            connection
                .getMetaData()
                .getTables(connection.getCatalog(), connection.getSchema(), "%", null)) {
      while (tables.next()) if (table.equalsIgnoreCase(tables.getString("TABLE_NAME"))) return true;
      return false;
    }
  }
}
