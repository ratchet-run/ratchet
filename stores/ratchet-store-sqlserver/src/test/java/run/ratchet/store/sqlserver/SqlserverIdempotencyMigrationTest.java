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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import run.ratchet.store.migration.SchemaMigrator;

/** Exercises the new-table and repeat-application paths with a pre-upgrade job. */
class SqlserverIdempotencyMigrationTest {
  @Test
  void backfillsExistingJobAndKeepsItsOriginalOwnerOnRepeat() throws Exception {
    var fixture = new SqlserverTestFixture();
    Throwable primaryFailure = null;
    try {
      fixture.cleanupStore();
      var job = fixture.store().create(fixture.newPendingJob());
      var dataSource = mock(DataSource.class);
      when(dataSource.getConnection()).thenAnswer(ignored -> fixture.openConnection());
      var dialect = new SqlserverSchemaMigrationDialect();
      var migrator = new SchemaMigrator(dataSource, dialect);
      try (var connection = fixture.openConnection();
          var statement = connection.createStatement()) {
        // Migrations normally run in order. This test runs V007 against the current schema,
        // so reproduce V007's own column shape before applying it.
        statement.execute(
            """
            ALTER TABLE scheduler_job DROP CONSTRAINT uk_idempotency_key;
            ALTER TABLE scheduler_job ALTER COLUMN idempotency_key
              VARCHAR(36) COLLATE DATABASE_DEFAULT NOT NULL;
            ALTER TABLE scheduler_job ADD CONSTRAINT uk_idempotency_key UNIQUE (idempotency_key);
            """);
        statement.execute("DROP TABLE scheduler_idempotency_key");
        statement.execute(dialect.createVersionTableSql());
        statement.executeUpdate("DELETE FROM ratchet_schema_version");
        // The fixture installs the consolidated schema. Record all other versions so that the
        // bundled migrator exercises exactly V007, including its statement-splitting path.
        try (var record = connection.prepareStatement(dialect.recordVersionSql())) {
          for (var script : migrator.discoverMigrations()) {
            if (script.version().equals("007")) continue;
            record.setString(1, script.version());
            record.setString(2, script.description());
            record.setString(3, script.checksum());
            record.executeUpdate();
          }
        }
      }
      assertEquals(
          List.of("007"),
          migrator.migrate().applied().stream()
              .map(SchemaMigrator.MigrationScript::version)
              .toList());
      assertEquals(List.of(), migrator.migrate().applied());
      restoreCurrentIdempotencyShape(fixture);
      // Reapply against an existing ledger table to cover the guarded DDL and backfill paths.
      try (var connection = fixture.openConnection();
          var statement = connection.createStatement()) {
        statement.executeUpdate("DELETE FROM ratchet_schema_version WHERE version = '007'");
      }
      assertEquals(
          List.of("007"),
          migrator.migrate().applied().stream()
              .map(SchemaMigrator.MigrationScript::version)
              .toList());
      assertEquals(
          Optional.of(job.getId()),
          fixture.store().findOriginalJobIdByIdempotencyKey(job.getIdempotencyKey()));
      fixture.store().delete(job.getId());
      assertEquals(
          Optional.of(job.getId()),
          fixture.store().findOriginalJobIdByIdempotencyKey(job.getIdempotencyKey()));
    } catch (Exception | AssertionError failure) {
      primaryFailure = failure;
      throw failure;
    } finally {
      try {
        restoreCurrentIdempotencyShape(fixture);
        fixture.cleanupStore();
      } catch (Exception | AssertionError cleanupFailure) {
        if (primaryFailure == null) throw cleanupFailure;
        primaryFailure.addSuppressed(cleanupFailure);
      }
    }
  }

  private static void restoreCurrentIdempotencyShape(SqlserverTestFixture fixture)
      throws SQLException {
    try (var connection = fixture.openConnection();
        var statement = connection.createStatement()) {
      statement.execute(
          """
          IF OBJECT_ID('scheduler_idempotency_key', 'U') IS NULL
          BEGIN
            CREATE TABLE scheduler_idempotency_key (
              idempotency_key NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL,
              original_job_id BINARY(16) NOT NULL,
              reserved_at DATETIME2(6) NOT NULL,
              CONSTRAINT pk_scheduler_idempotency PRIMARY KEY (idempotency_key)
            );
          END;
          IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('scheduler_job') AND name = 'idempotency_key'
              AND (system_type_id <> TYPE_ID('nvarchar')
                OR collation_name <> 'Latin1_General_100_BIN2')
          )
          BEGIN
            ALTER TABLE scheduler_job DROP CONSTRAINT IF EXISTS uk_idempotency_key;
            ALTER TABLE scheduler_job ALTER COLUMN idempotency_key
              NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL;
            ALTER TABLE scheduler_job ADD CONSTRAINT uk_idempotency_key UNIQUE (idempotency_key);
          END;
          IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('scheduler_idempotency_key') AND name = 'idempotency_key'
              AND (system_type_id <> TYPE_ID('nvarchar')
                OR collation_name <> 'Latin1_General_100_BIN2')
          )
          BEGIN
            ALTER TABLE scheduler_idempotency_key
              DROP CONSTRAINT IF EXISTS pk_scheduler_idempotency;
            ALTER TABLE scheduler_idempotency_key ALTER COLUMN idempotency_key
              NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL;
            ALTER TABLE scheduler_idempotency_key ADD CONSTRAINT pk_scheduler_idempotency
              PRIMARY KEY (idempotency_key);
          END;
          """);
    }
  }
}
