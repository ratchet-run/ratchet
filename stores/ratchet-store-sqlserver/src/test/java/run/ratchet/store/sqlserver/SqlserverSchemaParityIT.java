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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.JdbcDriverDataSource;

/** Compares physical metadata from the migration chain and the contract-test init script. */
class SqlserverSchemaParityIT {
  @Test
  void migratedAndConsolidatedSchemasHaveIdenticalMetadata() throws Exception {
    try (MSSQLServerContainer container = MssqlContainers.create()) {
      container.start();
      try (Connection c =
              DriverManager.getConnection(
                  container.getJdbcUrl(), container.getUsername(), container.getPassword());
          var statement = c.createStatement()) {
        statement.executeUpdate("CREATE DATABASE migrated");
        statement.executeUpdate("CREATE DATABASE consolidated");
      }
      var source =
          new JdbcDriverDataSource(
              container.getJdbcUrl() + ";databaseName=migrated",
              container.getUsername(),
              container.getPassword());
      new SchemaMigrator(source, new SqlserverSchemaMigrationDialect()).migrate();
      try (Connection migrated = source.getConnection();
          Connection consolidated =
              DriverManager.getConnection(
                  container.getJdbcUrl() + ";databaseName=consolidated",
                  container.getUsername(),
                  container.getPassword())) {
        String schema;
        try (var input =
            getClass().getClassLoader().getResourceAsStream("ddl/sqlserver-schema.sql")) {
          if (input == null) throw new IllegalStateException("missing SQL Server schema");
          schema = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        // The init script deliberately contains flat statements and no procedural batches.
        schema = schema.replaceAll("(?m)--.*$", "");
        try (var statement = consolidated.createStatement()) {
          for (String sql : schema.split(";")) {
            if (!sql.isBlank()) statement.execute(sql);
          }
        }
        for (String query : List.of(COLUMNS, INDEXES, CONSTRAINTS)) {
          assertEquals(metadata(migrated, query), metadata(consolidated, query), query);
        }
      }
    }
  }

  private static List<List<String>> metadata(Connection c, String sql) throws Exception {
    var result = new ArrayList<List<String>>();
    try (var statement = c.createStatement();
        var rows = statement.executeQuery(sql)) {
      while (rows.next()) {
        var row = new ArrayList<String>();
        for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) row.add(rows.getString(i));
        result.add(row);
      }
    }
    return result;
  }

  private static final String COLUMNS =
      """
      SELECT t.name, c.name, TYPE_NAME(c.system_type_id), c.max_length, c.is_nullable,
             c.collation_name, c.is_computed, cc.definition
      FROM sys.tables t JOIN sys.columns c ON c.object_id = t.object_id
      LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
      WHERE t.name LIKE 'scheduler[_]%' OR t.name = 'ratchet_schema_version'
      ORDER BY t.name, c.name
      """;

  private static final String INDEXES =
      """
      SELECT t.name, i.name, i.type, i.is_unique, i.is_primary_key, i.filter_definition,
             c.name, ic.key_ordinal, ic.is_descending_key, ic.is_included_column
      FROM sys.tables t JOIN sys.indexes i ON i.object_id = t.object_id
      JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
      JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
      WHERE t.name LIKE 'scheduler[_]%' OR t.name = 'ratchet_schema_version'
      ORDER BY t.name, i.name, ic.is_included_column, ic.key_ordinal, c.name
      """;

  // Names are excluded: V001 and the consolidated schema let SQL Server name some defaults.
  private static final String CONSTRAINTS =
      """
      SELECT t.name, 'CHECK' AS kind, c.name, k.definition
      FROM sys.tables t JOIN sys.check_constraints k ON k.parent_object_id = t.object_id
      LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = k.parent_column_id
      WHERE t.name LIKE 'scheduler[_]%' OR t.name = 'ratchet_schema_version'
      UNION ALL
      SELECT t.name, 'DEFAULT', c.name, d.definition
      FROM sys.tables t JOIN sys.default_constraints d ON d.parent_object_id = t.object_id
      JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = d.parent_column_id
      WHERE t.name LIKE 'scheduler[_]%' OR t.name = 'ratchet_schema_version'
      ORDER BY 1, 2, 3, 4
      """;
}
