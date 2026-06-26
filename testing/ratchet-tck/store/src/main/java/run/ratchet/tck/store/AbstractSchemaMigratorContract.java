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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;

/**
 * Shared schema migrator integration contract for JDBC stores. Implementations provide only
 * dialect-specific database lifecycle plumbing; this contract owns cross-dialect migration
 * behavior.
 */
public abstract class AbstractSchemaMigratorContract {

  protected abstract DataSource dataSource();

  protected abstract SchemaMigrationDialect dialect();

  protected abstract void resetDatabase() throws Exception;

  protected abstract Connection newJdbcConnection() throws SQLException;

  @Test
  void migratesVirginSchemaThenIsIdempotentOnRerun() throws Exception {
    resetDatabase();

    SchemaMigrator.MigrationResult first = newMigrator().migrate();
    assertTrue(first.appliedCount() > 0, "expected at least one migration applied");
    assertEquals(0, first.skippedCount());
    assertTrue(tableExists("scheduler_job_queue"), "core table should exist after migration");

    assertSchemaVersionRowsMatch(first.applied());

    SchemaMigrator.MigrationResult second = newMigrator().migrate();
    assertEquals(0, second.appliedCount());
    assertEquals(first.appliedCount(), second.skippedCount());
    assertSchemaVersionRowsMatch(second.skipped());
  }

  @Test
  @SuppressWarnings("AutoCloseableResource")
  void parallelMigratorsConvergeUnderAdvisoryLock() throws Exception {
    resetDatabase();

    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch go = new CountDownLatch(1);
    Callable<SchemaMigrator.MigrationResult> task =
        () -> {
          ready.countDown();
          go.await();
          return newMigrator().migrate();
        };
    // Java 17 ExecutorService is not AutoCloseable; the finally block below already shuts the
    // pool down and awaits termination.
    ExecutorService pool = Executors.newFixedThreadPool(2);
    List<Future<SchemaMigrator.MigrationResult>> futures = new ArrayList<>();
    int totalApplied = 0;
    int totalSkipped = 0;
    try {
      futures.add(pool.submit(task));
      futures.add(pool.submit(task));
      assertTrue(ready.await(15, TimeUnit.SECONDS));
      go.countDown();

      for (Future<SchemaMigrator.MigrationResult> future : futures) {
        SchemaMigrator.MigrationResult result = future.get(120, TimeUnit.SECONDS);
        totalApplied += result.appliedCount();
        totalSkipped += result.skippedCount();
      }
    } finally {
      for (Future<SchemaMigrator.MigrationResult> future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      pool.shutdown();
      if (!pool.awaitTermination(15, TimeUnit.SECONDS)) {
        pool.shutdownNow();
        assertTrue(
            pool.awaitTermination(15, TimeUnit.SECONDS), "Executor should terminate cleanly");
      }
    }

    List<SchemaMigrator.MigrationScript> expected = newMigrator().discoverMigrations();
    assertSchemaVersionRowsMatch(expected);
    assertEquals(expected.size(), totalApplied, "applied total must equal final version-row count");
    assertEquals(expected.size(), totalSkipped, "skipped total must equal final version-row count");
  }

  protected final SchemaMigrator newMigrator() {
    return new SchemaMigrator(dataSource(), dialect());
  }

  protected boolean tableExists(String name) throws SQLException {
    try (Connection c = newJdbcConnection();
        ResultSet rs = c.getMetaData().getTables(null, null, name, new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  protected final void assertSchemaVersionRowsMatch(
      List<SchemaMigrator.MigrationScript> expectedScripts) throws SQLException {
    List<SchemaVersionRow> actual = schemaVersionRows();
    List<SchemaVersionRow> expected =
        expectedScripts.stream()
            .map(
                script ->
                    new SchemaVersionRow(script.version(), script.description(), script.checksum()))
            .sorted(Comparator.comparingInt(SchemaVersionRow::numericVersion))
            .toList();

    assertEquals(expected, actual, "schema version ledger should match migration scripts exactly");
    assertEquals(
        actual.size(),
        actual.stream().map(SchemaVersionRow::version).distinct().count(),
        "schema version ledger should not contain duplicate versions");
    assertTrue(
        actual.stream().allMatch(row -> row.checksum() != null && !row.checksum().isBlank()),
        "schema version ledger should record every migration checksum");
    for (int i = 0; i < actual.size(); i++) {
      assertEquals(i + 1, actual.get(i).numericVersion(), "schema version ledger has a gap");
    }
  }

  private List<SchemaVersionRow> schemaVersionRows() throws SQLException {
    try (Connection c = newJdbcConnection();
        PreparedStatement s =
            c.prepareStatement(
                "SELECT version, description, checksum FROM ratchet_schema_version ORDER BY version");
        ResultSet rs = s.executeQuery()) {
      List<SchemaVersionRow> rows = new ArrayList<>();
      while (rs.next()) {
        rows.add(new SchemaVersionRow(rs.getString(1), rs.getString(2), rs.getString(3)));
      }
      return rows;
    }
  }

  private record SchemaVersionRow(String version, String description, String checksum) {

    private int numericVersion() {
      return Integer.parseInt(version);
    }
  }
}
