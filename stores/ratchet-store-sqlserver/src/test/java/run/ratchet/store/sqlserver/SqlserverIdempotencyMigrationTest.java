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

import java.util.List;
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
          java.util.Optional.of(job.getId()),
          fixture.store().findOriginalJobIdByIdempotencyKey(job.getIdempotencyKey()));
      fixture.store().delete(job.getId());
      assertEquals(
          java.util.Optional.of(job.getId()),
          fixture.store().findOriginalJobIdByIdempotencyKey(job.getIdempotencyKey()));
    } catch (Exception | AssertionError failure) {
      primaryFailure = failure;
      throw failure;
    } finally {
      try {
        fixture.cleanupStore();
      } catch (RuntimeException cleanupFailure) {
        if (primaryFailure == null) throw cleanupFailure;
        primaryFailure.addSuppressed(cleanupFailure);
      }
    }
  }
}
