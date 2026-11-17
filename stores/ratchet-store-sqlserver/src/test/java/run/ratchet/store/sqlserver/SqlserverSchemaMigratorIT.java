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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.AbstractSchemaMigratorContract;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Runs {@link SchemaMigrator} against a virgin SQL Server Testcontainer (no init script). Verifies
 * that the migrator brings the schema up from empty, is idempotent on re-run, and converges under
 * {@code sp_getapplock} when two threads race.
 */
class SqlserverSchemaMigratorIT extends AbstractSchemaMigratorContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final MSSQLServerContainer CONTAINER = MssqlContainers.create();

  @BeforeAll
  static void start() {
    CONTAINER.start();
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  @Override
  protected DataSource dataSource() {
    return new JdbcDriverDataSource(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @Override
  protected SchemaMigrationDialect dialect() {
    return new SqlserverSchemaMigrationDialect();
  }

  @Override
  protected Connection newJdbcConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  // Drop every Ratchet table child-first so foreign keys never block the drop. SQL Server has no
  // "DROP SCHEMA ... CASCADE", so the virgin state is restored table by table.
  private static final List<String> TABLES_CHILD_FIRST =
      List.of(
          "scheduler_partition",
          "scheduler_partition_set",
          "scheduler_business_key_reservation",
          "scheduler_job_queue",
          "scheduler_event_outbox",
          "scheduler_job_tag",
          "scheduler_job_properties",
          "scheduler_job_extension_state",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_waiter",
          "scheduler_resource_permit",
          "scheduler_workflow_condition",
          "scheduler_batch_metrics",
          "scheduler_batch",
          "scheduler_job_archive",
          "scheduler_job",
          "scheduler_recurring_job_archive",
          "scheduler_recurring_job",
          "scheduler_resource_limit",
          "scheduler_lock",
          "scheduler_node",
          "ratchet_schema_version");

  @Override
  protected void resetDatabase() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      for (String table : TABLES_CHILD_FIRST) {
        s.execute("DROP TABLE IF EXISTS dbo." + table);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "scheduler_job",
        "scheduler_job_queue",
        "scheduler_recurring_job",
        "scheduler_business_key_reservation",
        "scheduler_job_archive",
        "scheduler_recurring_job_archive",
        "scheduler_event_outbox"
      })
  void unicodeMigrationPreservesExistingRowsAndIsNoOpOnRepeat(String table) throws Exception {
    migrateThrough17();
    if (!table.equals("scheduler_business_key_reservation"))
      insertBusinessKeyRow(table, "ascii-key");
    try (var c = newJdbcConnection();
        var statement = c.createStatement()) {
      statement.executeUpdate(
          """
          INSERT INTO scheduler_business_key_reservation
            (business_key, owner_job_id, owner_table, held_by_node)
          VALUES ('ascii-key', 0x00000000000000000000000000000001, 'QUEUE', 'ascii-node')
          """);
      statement.executeUpdate(
          "CREATE STATISTICS unicode_key_stats ON scheduler_business_key_reservation (business_key,"
              + " held_by_node) WITH NORECOMPUTE");
    }
    var migrator = new SchemaMigrator(dataSource(), dialect());
    assertEquals(
        List.of("018"),
        migrator.migrate().applied().stream()
            .map(SchemaMigrator.MigrationScript::version)
            .toList());
    try (var c = newJdbcConnection();
        var statement = c.createStatement()) {
      try (var rows = statement.executeQuery("SELECT business_key FROM " + table)) {
        assertTrue(rows.next());
        assertEquals("ascii-key", rows.getString(1));
        assertFalse(rows.next());
      }
      try (var rows =
          statement.executeQuery(
              "SELECT no_recompute FROM sys.stats WHERE name = 'unicode_key_stats'")) {
        assertTrue(rows.next());
        assertTrue(rows.getBoolean(1));
      }
      try (var rows =
          statement.executeQuery(
              "SELECT business_key, held_by_node FROM scheduler_business_key_reservation")) {
        assertTrue(rows.next());
        assertEquals("ascii-key", rows.getString(1));
        assertEquals("ascii-node", rows.getString(2));
        assertFalse(rows.next());
      }
      try (var rows =
          statement.executeQuery(
              """
              SELECT TYPE_NAME(system_type_id), max_length FROM sys.columns
              WHERE object_id = OBJECT_ID('scheduler_business_key_reservation') AND name = 'business_key'
              """)) {
        assertTrue(rows.next());
        assertEquals("nvarchar", rows.getString(1));
        assertEquals(510, rows.getInt(2));
      }
      for (String expected :
          List.of(
              "scheduler_node.node_id:128:0",
              "scheduler_lock.lock_name:256:0",
              "scheduler_lock.owner_node:128:0",
              "scheduler_resource_limit.resource_name:200:0",
              "scheduler_resource_limit.description:510:1",
              "scheduler_recurring_job.business_key:510:1",
              "scheduler_recurring_job.resource_name:200:1",
              "scheduler_recurring_job.execution_target:128:1",
              "scheduler_recurring_job.caller_principal:510:1",
              "scheduler_recurring_job_archive.business_key:510:1",
              "scheduler_recurring_job_archive.execution_target:128:1",
              "scheduler_recurring_job_archive.caller_principal:510:1",
              "scheduler_job.idempotency_key:72:0",
              "scheduler_job.business_key:510:1",
              "scheduler_job.resource_name:200:1",
              "scheduler_job.execution_target:128:1",
              "scheduler_job.caller_principal:510:1",
              "scheduler_job_queue.business_key:510:1",
              "scheduler_job_queue.picked_by:128:1",
              "scheduler_job_queue.signal_key:510:1",
              "scheduler_job_queue.signal_delivered_by:510:1",
              "scheduler_job_queue.execution_target:128:1",
              "scheduler_business_key_reservation.business_key:510:0",
              "scheduler_business_key_reservation.held_by_node:128:1",
              "scheduler_job_tag.tag:128:0",
              "scheduler_job_execution.node_id:128:0",
              "scheduler_job_archive.business_key:510:1",
              "scheduler_job_archive.archived_by:128:1",
              "scheduler_job_archive.tags:1024:1",
              "scheduler_resource_permit.resource_name:200:0",
              "scheduler_resource_permit.node_id:128:0",
              "scheduler_idempotency_key.idempotency_key:72:0",
              "scheduler_event_outbox.business_key:510:1",
              "scheduler_event_outbox.node_id:128:1",
              "scheduler_partition_set.set_key:72:0",
              "scheduler_partition_set.name:510:0",
              "scheduler_partition_set.owner_principal:510:1",
              "scheduler_resource_waiter.resource_name:200:0")) {
        String[] parts = expected.split("[:.]");
        try (var column =
            c.prepareStatement(
                """
                SELECT TYPE_NAME(system_type_id), max_length, is_nullable FROM sys.columns
                WHERE object_id = OBJECT_ID(?) AND name = ?
                """)) {
          column.setString(1, parts[0]);
          column.setString(2, parts[1]);
          try (var rows = column.executeQuery()) {
            assertTrue(rows.next(), expected);
            assertEquals("nvarchar", rows.getString(1), expected);
            assertEquals(Integer.parseInt(parts[2]), rows.getInt(2), expected);
            assertEquals(Integer.parseInt(parts[3]), rows.getInt(3), expected);
          }
        }
      }
      statement.executeUpdate("DELETE FROM ratchet_schema_version WHERE version = '018'");
    }
    assertEquals(1, migrator.migrate().appliedCount());
    assertEquals(0, migrator.migrate().appliedCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "scheduler_job",
        "scheduler_job_queue",
        "scheduler_recurring_job",
        "scheduler_business_key_reservation",
        "scheduler_job_archive",
        "scheduler_recurring_job_archive",
        "scheduler_event_outbox"
      })
  void unicodeMigrationRejectsLongBusinessKeyBeforeChangingSchema(String table) throws Exception {
    migrateThrough17();
    insertBusinessKeyRow(table, "x".repeat(255) + " ");
    var failure =
        assertThrows(
            SQLException.class, () -> new SchemaMigrator(dataSource(), dialect()).migrate());
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage());
    }
    assertTrue(
        messages.toString().contains(table + ".business_key exceeds 255"), messages.toString());
    try (var c = newJdbcConnection();
        var statement = c.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id ="
                    + " OBJECT_ID('scheduler_node') AND name = 'node_id'")) {
      assertTrue(rows.next());
      assertEquals("varchar", rows.getString(1));
    }
  }

  private void insertBusinessKeyRow(String table, String key) throws Exception {
    String id = "0x00000000000000000000000000000001";
    String columns;
    String values;
    switch (table) {
      case "scheduler_job" -> {
        columns = "job_id, job_type, payload, idempotency_key";
        values = id + ", 'SINGLE', N'{}', 'ascii-idem'";
      }
      case "scheduler_job_queue" -> {
        insertBusinessKeyRow("scheduler_job", "parent-key");
        columns = "job_id, job_type, scheduled_time";
        values = id + ", 'SINGLE', SYSUTCDATETIME()";
      }
      case "scheduler_recurring_job" -> {
        columns = "id, cron_expr, next_fire, payload";
        values = id + ", '* * * * *', SYSUTCDATETIME(), N'{}'";
      }
      case "scheduler_recurring_job_archive" -> {
        columns = "id, cron_expr, zone_id, payload, created_at, archive_reason";
        values = id + ", '* * * * *', 'UTC', N'{}', SYSUTCDATETIME(), 'CANCELED'";
      }
      case "scheduler_job_archive" -> {
        columns =
            "archive_id, original_job_id, final_status, job_type, priority,"
                + " original_scheduled_time, original_created_at";
        values = id + ", " + id + ", 'CANCELED', 'SINGLE', 2, SYSUTCDATETIME(), SYSUTCDATETIME()";
      }
      case "scheduler_event_outbox" -> {
        columns = "event_id, event_type, job_id, event_ts, retry_attempt, created_at";
        values = id + ", 'CANCELED', " + id + ", SYSUTCDATETIME(), 0, SYSUTCDATETIME()";
      }
      case "scheduler_business_key_reservation" -> {
        columns = "owner_job_id, owner_table";
        values = id + ", 'QUEUE'";
      }
      default -> throw new IllegalArgumentException(table);
    }
    try (var c = newJdbcConnection();
        var statement =
            c.prepareStatement(
                "INSERT INTO "
                    + table
                    + " (business_key, "
                    + columns
                    + ") VALUES (?, "
                    + values
                    + ")")) {
      statement.setString(1, key);
      statement.executeUpdate();
    }
  }

  private void migrateThrough17() throws Exception {
    resetDatabase();
    var migrator = new SchemaMigrator(dataSource(), dialect());
    try (var c = newJdbcConnection();
        var statement = c.createStatement()) {
      statement.execute(dialect().createVersionTableSql());
      for (var script : migrator.discoverMigrations()) {
        if (script.version().equals("018")) break;
        boolean result = statement.execute(script.sql());
        while (result || statement.getUpdateCount() != -1) {
          if (result) statement.getResultSet().close();
          result = statement.getMoreResults();
        }
        try (var record = c.prepareStatement(dialect().recordVersionSql())) {
          record.setString(1, script.version());
          record.setString(2, script.description());
          record.setString(3, script.checksum());
          record.executeUpdate();
        }
      }
    }
  }
}
