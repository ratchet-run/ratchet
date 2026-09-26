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
package run.ratchet.store.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.schema.RatchetSchemaCatalog;

/**
 * Exercises the dialect-agnostic {@link SchemaMigrator} engine — discovery, checksum validation,
 * the apply loop, and lock orchestration — against a {@link RecordingDialect} stub.
 * Dialect-specific SQL (lock statements, version DDL, upserts) is covered by each store's own
 * dialect test.
 */
class SchemaMigratorTest {

  private DataSource dataSource;
  private Connection connection;
  private Statement statement;
  private PreparedStatement selectVersion;
  private PreparedStatement selectHistory;
  private PreparedStatement insertVersion;
  private RecordingDialect dialect;

  private static ResultSet missingVersion() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    return resultSet;
  }

  private static ResultSet existingVersion(String checksum) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn(checksum);
    return resultSet;
  }

  private static ResultSet rows(String label, List<String> values) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    AtomicInteger index = new AtomicInteger(-1);
    when(resultSet.next()).thenAnswer(ignored -> index.incrementAndGet() < values.size());
    when(resultSet.getString(label)).thenAnswer(ignored -> values.get(index.get()));
    return resultSet;
  }

  private static ResultSet metadataRows(List<MetadataRow> rows) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    AtomicInteger index = new AtomicInteger(-1);
    when(resultSet.next()).thenAnswer(ignored -> index.incrementAndGet() < rows.size());
    when(resultSet.getString(ArgumentMatchers.anyString()))
        .thenAnswer(
            invocation -> {
              MetadataRow row = rows.get(index.get());
              return switch (invocation.getArgument(0, String.class)) {
                case "TABLE_CAT" -> row.catalog();
                case "TABLE_SCHEM" -> row.schema();
                case "TABLE_NAME" -> row.table();
                case "COLUMN_NAME" -> row.column();
                default -> null;
              };
            });
    return resultSet;
  }

  private static List<MetadataRow> tableRows(List<String> tables, String catalog, String schema) {
    return tables.stream().map(table -> new MetadataRow(catalog, schema, table, null)).toList();
  }

  private static List<MetadataRow> columnRows(
      String table, List<String> columns, String catalog, String schema) {
    return columns.stream().map(column -> new MetadataRow(catalog, schema, table, column)).toList();
  }

  private record MetadataRow(String catalog, String schema, String table, String column) {}

  private static ResultSet primaryKeyRows(List<String> columns) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    AtomicInteger index = new AtomicInteger(-1);
    when(resultSet.next()).thenAnswer(ignored -> index.incrementAndGet() < columns.size());
    when(resultSet.getString("COLUMN_NAME")).thenAnswer(ignored -> columns.get(index.get()));
    when(resultSet.getShort("KEY_SEQ")).thenAnswer(ignored -> (short) (index.get() + 1));
    return resultSet;
  }

  private static ResultSet uniqueIndexRows(String name, List<String> columns) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    AtomicInteger index = new AtomicInteger(-1);
    when(resultSet.next()).thenAnswer(ignored -> index.incrementAndGet() < columns.size());
    when(resultSet.getString("INDEX_NAME")).thenReturn(name);
    when(resultSet.getString("COLUMN_NAME")).thenAnswer(ignored -> columns.get(index.get()));
    when(resultSet.getShort("ORDINAL_POSITION")).thenAnswer(ignored -> (short) (index.get() + 1));
    return resultSet;
  }

  private static ResultSet historyRows(List<List<String>> rows) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    AtomicInteger index = new AtomicInteger(-1);
    when(resultSet.next()).thenAnswer(ignored -> index.incrementAndGet() < rows.size());
    when(resultSet.getString(1)).thenAnswer(ignored -> rows.get(index.get()).get(0));
    when(resultSet.getString(2)).thenAnswer(ignored -> rows.get(index.get()).get(1));
    return resultSet;
  }

  private static int indexOfContaining(List<String> values, String needle) {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i).contains(needle)) {
        return i;
      }
    }
    return -1;
  }

  private SchemaMigrator migrator(String classpathPrefix) {
    return new SchemaMigrator(dataSource, dialect, classpathPrefix);
  }

  private static ClassLoader resourceProtocolClassLoader(String classpathPrefix) throws Exception {
    URL resourceRoot =
        new URL(
            null,
            "resource:/" + classpathPrefix,
            new URLStreamHandler() {
              @Override
              protected URLConnection openConnection(URL url) {
                throw new UnsupportedOperationException("Directory URL must not be opened");
              }
            });
    return new ClassLoader(SchemaMigratorTest.class.getClassLoader()) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        if (name.equals(classpathPrefix)) {
          return Collections.enumeration(List.of(resourceRoot));
        }
        return super.getResources(name);
      }
    };
  }

  @BeforeEach
  void setUp() throws Exception {
    dataSource = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    selectVersion = mock(PreparedStatement.class);
    selectHistory = mock(PreparedStatement.class);
    insertVersion = mock(PreparedStatement.class);
    dialect = new RecordingDialect();
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.prepareStatement(startsWith("SELECT checksum"))).thenReturn(selectVersion);
    when(connection.prepareStatement(startsWith("SELECT version, checksum")))
        .thenReturn(selectHistory);
    when(connection.prepareStatement(startsWith("INSERT INTO ratchet_schema_version")))
        .thenReturn(insertVersion);
    configureCurrentSchemaMetadata(metadata, false);
  }

  private void configureCurrentSchemaMetadata(DatabaseMetaData metadata, boolean versionTable)
      throws Exception {
    configureCurrentSchemaMetadata(metadata, versionTable, null, null);
  }

  private void configureCurrentSchemaMetadata(
      DatabaseMetaData metadata, boolean versionTable, String catalog, String schema)
      throws Exception {
    List<String> tables =
        new ArrayList<>(
            RatchetSchemaCatalog.CURRENT.tables().stream().map(table -> table.name()).toList());
    if (versionTable) {
      tables.add("ratchet_schema_version");
    }
    when(metadata.getTables(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
        .thenAnswer(ignored -> metadataRows(tableRows(tables, catalog, schema)));
    Map<String, List<String>> columns = new HashMap<>();
    RatchetSchemaCatalog.CURRENT
        .tables()
        .forEach(
            table ->
                columns.put(
                    table.name(), table.columns().stream().map(column -> column.name()).toList()));
    when(metadata.getColumns(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              String table = invocation.getArgument(2);
              return metadataRows(
                  columnRows(table, columns.getOrDefault(table, List.of()), catalog, schema));
            });
    Map<String, List<String>> primaryKeys = new HashMap<>();
    RatchetSchemaCatalog.CURRENT
        .tables()
        .forEach(table -> primaryKeys.put(table.name(), table.primaryKey()));
    when(metadata.getPrimaryKeys(
            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenAnswer(
            invocation ->
                primaryKeyRows(primaryKeys.getOrDefault(invocation.getArgument(2), List.of())));
    when(metadata.getIndexInfo(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(true)))
        .thenAnswer(
            invocation -> {
              String table = invocation.getArgument(2);
              if ("scheduler_job".equals(table)) {
                return uniqueIndexRows("uk_idempotency_key", List.of("idempotency_key"));
              }
              return uniqueIndexRows("unused", List.of());
            });
  }

  @Test
  void appliesClasspathMigrationsInVersionOrder() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    ResultSet secondMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion, secondMissingVersion);

    SchemaMigrator.MigrationResult result = migrator("schema-migrator").migrate();

    assertEquals(2, result.appliedCount());
    assertEquals(0, result.skippedCount());
    assertEquals(List.of("001", "002"), result.applied().stream().map(s -> s.version()).toList());

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(statement, atLeast(3)).execute(sqlCaptor.capture());
    List<String> executedSql = sqlCaptor.getAllValues();
    assertTrue(
        indexOfContaining(executedSql, "CREATE TABLE IF NOT EXISTS ratchet_schema_version") >= 0);
    assertTrue(
        indexOfContaining(executedSql, "CREATE TABLE ratchet_test_order")
            < indexOfContaining(executedSql, "INSERT INTO ratchet_test_order"));
    assertTrue(
        executedSql.stream()
            .anyMatch(sql -> sql.contains("second; still one") && sql.startsWith("INSERT")));

    verify(insertVersion, times(2)).executeUpdate();
    verify(connection, times(2)).commit();
    assertEquals(1, dialect.acquireCount());
    assertEquals(1, dialect.releaseCount());
  }

  @Test
  void discoversIndexedMigrationsWhenDirectoryUsesNativeResourceProtocol() throws Exception {
    SchemaMigrator migrator =
        new SchemaMigrator(
            dataSource, dialect, "schema-migrator", resourceProtocolClassLoader("schema-migrator"));

    List<SchemaMigrator.MigrationScript> scripts = migrator.discoverMigrations();

    assertEquals(List.of("001", "002"), scripts.stream().map(s -> s.version()).toList());
  }

  @Test
  void failsBeforeConnectingWhenNoMigrationScriptsAreDiscovered() throws Exception {
    SchemaMigrationException ex =
        assertThrows(
            SchemaMigrationException.class, () -> migrator("schema-migrator-empty").migrate());

    assertTrue(ex.getMessage().contains("No Ratchet schema migration scripts were discovered"));
    assertTrue(ex.getMessage().contains("schema-migrator-empty/index.txt"));
    verify(dataSource, never()).getConnection();
  }

  @Test
  void autoMigrationFailsLoudlyWhenNoMigrationScriptsAreDiscovered() throws Exception {
    @SuppressWarnings("unchecked")
    Instance<DataSource> dataSources = mock(Instance.class);
    when(dataSources.isUnsatisfied()).thenReturn(false);
    when(dataSources.isAmbiguous()).thenReturn(false);
    when(dataSources.get()).thenReturn(dataSource);

    @SuppressWarnings("unchecked")
    Instance<SchemaMigrationDialect> dialects = mock(Instance.class);
    when(dialects.isUnsatisfied()).thenReturn(false);
    when(dialects.iterator()).thenReturn(List.<SchemaMigrationDialect>of(dialect).iterator());

    RatchetOptions options =
        RatchetOptions.builder()
            .schema(
                schema ->
                    schema
                        .autoMigrate(true)
                        .migrationDialect("stub")
                        .migrationPrefix("schema-migrator-empty"))
            .build();
    SchemaMigrationLifecycleHook hook =
        new SchemaMigrationLifecycleHook(options, dataSources, dialects);

    SchemaInitializationException ex =
        assertThrows(SchemaInitializationException.class, hook::beforeStart);

    assertTrue(ex.getMessage().contains("No Ratchet schema migration scripts were discovered"));
    assertTrue(ex.getMessage().contains("schema-migrator-empty/index.txt"));
    verify(dataSource, never()).getConnection();
  }

  @Test
  void singleStatementDirectivePreservesJdbcBlocksWithInternalSemicolons() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);

    migrator("schema-migrator-single-statement").migrate();

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(statement, atLeast(2)).execute(sqlCaptor.capture());
    List<String> executedSql = sqlCaptor.getAllValues();
    List<String> blocks = executedSql.stream().filter(sql -> sql.startsWith("BEGIN")).toList();
    assertEquals(1, blocks.size());
    assertTrue(blocks.get(0).contains("EXECUTE IMMEDIATE 'SELECT 1 FROM dual';"));
    assertTrue(blocks.get(0).endsWith("END;"));
  }

  @Test
  void nativeBatchPreservesBlocksAndDrainsAllResults() throws Exception {
    dialect.nativeBatch = true;
    ResultSet missing = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(missing);
    when(statement.execute(startsWith("IF OBJECT_ID"))).thenReturn(true);
    when(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).thenReturn(false, false);
    when(statement.getUpdateCount()).thenReturn(3, -1);

    var migrator = migrator("schema-migrator-native-batch");
    var script = migrator.discoverMigrations().get(0);
    migrator.migrate();

    verify(statement).execute(script.sql());
    verify(statement, times(2)).getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    verify(insertVersion).executeUpdate();
    verify(connection).commit();
  }

  @Test
  void nativeBatchLateFailureRollsBackWithoutRecordingVersion() throws Exception {
    dialect.nativeBatch = true;
    ResultSet missing = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(missing);
    when(statement.getUpdateCount()).thenReturn(1);
    SQLException failure = new SQLException("later batch statement failed");
    when(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).thenThrow(failure);

    assertEquals(
        failure,
        assertThrows(SQLException.class, () -> migrator("schema-migrator-native-batch").migrate()));

    verify(connection).rollback();
    verify(connection, never()).commit();
    verify(insertVersion, never()).executeUpdate();
  }

  @Test
  void matchingNativeBatchChecksumSkipsExecution() throws Exception {
    dialect.nativeBatch = true;
    var migrator = migrator("schema-migrator-native-batch");
    var script = migrator.discoverMigrations().get(0);
    ResultSet existing = existingVersion(script.checksum());
    when(selectVersion.executeQuery()).thenReturn(existing);

    assertEquals(1, migrator.migrate().skippedCount());

    verify(statement, never()).execute(script.sql());
    verify(connection, never()).commit();
  }

  @Test
  void singleStatementDirectiveRejectsMissingSql() throws Exception {
    Method splitStatements =
        SchemaMigrator.class.getDeclaredMethod("splitStatements", String.class);
    splitStatements.setAccessible(true);

    InvocationTargetException thrown =
        assertThrows(
            InvocationTargetException.class,
            () -> splitStatements.invoke(null, "-- ratchet:single-statement"));

    assertTrue(thrown.getCause() instanceof SchemaMigrationException);
    assertEquals(
        "Single-statement migration directive must be followed by SQL",
        thrown.getCause().getMessage());
  }

  @Test
  void skipsMigrationsWithMatchingChecksums() throws Exception {
    SchemaMigrator migrator = migrator("schema-migrator");
    List<SchemaMigrator.MigrationScript> scripts = migrator.discoverMigrations();
    ResultSet firstExistingVersion = existingVersion(scripts.get(0).checksum());
    ResultSet secondExistingVersion = existingVersion(scripts.get(1).checksum());
    when(selectVersion.executeQuery()).thenReturn(firstExistingVersion, secondExistingVersion);

    SchemaMigrator.MigrationResult result = migrator.migrate();

    assertEquals(0, result.appliedCount());
    assertEquals(2, result.skippedCount());
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void validatesInstalledMigrationsWithoutWritingOrLocking() throws Exception {
    configureCurrentSchemaMetadata(connection.getMetaData(), true);
    SchemaMigrator migrator = migrator("schema-migrator");
    List<SchemaMigrator.MigrationScript> scripts = migrator.discoverMigrations();
    ResultSet history =
        historyRows(
            List.of(
                List.of(scripts.get(0).version(), scripts.get(0).checksum()),
                List.of(scripts.get(1).version(), scripts.get(1).checksum())));
    when(selectHistory.executeQuery()).thenReturn(history);

    SchemaMigrator.ValidationResult result = migrator.validate();

    verify(connection, atMost(1)).getCatalog();
    verify(connection, atMost(1)).getSchema();
    assertEquals(2, result.validated().size());
    assertEquals(List.of("001", "002"), result.validated().stream().map(s -> s.version()).toList());
    verify(statement, never()).execute(ArgumentMatchers.anyString());
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
    assertEquals(0, dialect.acquireCount());
    assertEquals(0, dialect.releaseCount());
  }

  @Test
  void validationAllowsAnExternallyManagedSchemaWithoutMigrationHistory() throws Exception {
    SchemaMigrator migrator = migrator("schema-migrator");
    SchemaMigrator.ValidationResult result = migrator.validate();

    assertEquals(0, result.validated().size());
    verify(statement, never()).execute(ArgumentMatchers.anyString());
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void validationRejectsMissingRequiredTable() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    ResultSet noTables = rows("TABLE_NAME", List.of());
    when(metadata.getTables(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
        .thenReturn(noTables);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
    verify(connection, never()).prepareStatement(startsWith("SELECT version, checksum"));
  }

  @Test
  void validationFiltersSiblingTableColumnsFromRawMetadataPattern() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    List<String> targetColumns = schedulerJobColumnsWithoutPayload();
    List<MetadataRow> patternMatches =
        new ArrayList<>(columnRows("scheduler_job", targetColumns, null, null));
    patternMatches.add(new MetadataRow(null, null, "schedulerXjob", "payload"));
    ResultSet wildcardTableRows = metadataRows(patternMatches);
    when(metadata.getColumns(any(), any(), eq("scheduler_job"), eq("%")))
        .thenReturn(wildcardTableRows);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
    assertTrue(ex.getMessage().contains("payload"));
    verify(metadata).getColumns(any(), any(), eq("scheduler_job"), eq("%"));
  }

  @Test
  void validationFiltersSiblingSchemaColumnsFromRawMetadataPattern() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    String schema = "ratchet_schema";
    List<String> tables =
        RatchetSchemaCatalog.CURRENT.tables().stream().map(table -> table.name()).toList();
    ResultSet selectedSchemaTables = metadataRows(tableRows(tables, null, schema));
    when(connection.getSchema()).thenReturn(schema);
    when(metadata.getTables(any(), eq(schema), eq("%"), ArgumentMatchers.isNull()))
        .thenReturn(selectedSchemaTables);

    List<MetadataRow> patternMatches =
        new ArrayList<>(
            columnRows("scheduler_job", schedulerJobColumnsWithoutPayload(), null, schema));
    patternMatches.add(new MetadataRow(null, "ratchetXschema", "scheduler_job", "payload"));
    ResultSet wildcardSchemaRows = metadataRows(patternMatches);
    when(metadata.getColumns(any(), eq(schema), eq("scheduler_job"), eq("%")))
        .thenReturn(wildcardSchemaRows);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
    assertTrue(ex.getMessage().contains("payload"));
    verify(metadata).getTables(any(), eq(schema), eq("%"), ArgumentMatchers.isNull());
    verify(metadata).getColumns(any(), eq(schema), eq("scheduler_job"), eq("%"));
  }

  @Test
  void validationFoldsCanonicalizedLowercaseCatalogMetadata() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    String requestedCatalog = "RatchetDB";
    String metadataCatalog = "ratchetdb";
    configureCurrentSchemaMetadata(metadata, false, metadataCatalog, null);
    when(connection.getCatalog()).thenReturn(requestedCatalog);
    when(metadata.storesLowerCaseIdentifiers()).thenReturn(true);
    when(metadata.supportsMixedCaseIdentifiers()).thenReturn(false);

    SchemaMigrator.ValidationResult result = migrator("schema-migrator").validate();

    assertEquals(0, result.validated().size());
    verify(metadata).getTables(eq(requestedCatalog), any(), eq("%"), ArgumentMatchers.isNull());
  }

  @Test
  void validationFoldsCanonicalizedMixedCaseCatalogMetadataWhenMixedCaseIsUnsupported()
      throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    String requestedCatalog = "RatchetDB";
    String metadataCatalog = "ratchetdb";
    configureCurrentSchemaMetadata(metadata, false, metadataCatalog, null);
    when(connection.getCatalog()).thenReturn(requestedCatalog);
    when(metadata.storesMixedCaseIdentifiers()).thenReturn(true);
    when(metadata.supportsMixedCaseIdentifiers()).thenReturn(false);

    SchemaMigrator.ValidationResult result = migrator("schema-migrator").validate();

    assertEquals(0, result.validated().size());
    verify(metadata).getTables(eq(requestedCatalog), any(), eq("%"), ArgumentMatchers.isNull());
  }

  @Test
  void validationRejectsCaseOnlyCatalogSiblingWhenMetadataPreservesCase() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    String catalog = "RatchetDB";
    List<String> tables =
        RatchetSchemaCatalog.CURRENT.tables().stream()
            .map(table -> table.name())
            .filter(table -> !table.equals("scheduler_job"))
            .toList();
    List<MetadataRow> metadataRows = new ArrayList<>(tableRows(tables, catalog, null));
    metadataRows.add(new MetadataRow("ratchetdb", null, "scheduler_job", null));
    ResultSet caseOnlySiblingTables = metadataRows(metadataRows);
    when(connection.getCatalog()).thenReturn(catalog);
    when(metadata.storesMixedCaseIdentifiers()).thenReturn(true);
    when(metadata.supportsMixedCaseIdentifiers()).thenReturn(true);
    when(metadata.getTables(eq(catalog), any(), eq("%"), ArgumentMatchers.isNull()))
        .thenReturn(caseOnlySiblingTables);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
  }

  @Test
  void validationFiltersRequiredTablesFromAnotherCatalog() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    String catalog = "ratchet_catalog";
    List<String> tables =
        RatchetSchemaCatalog.CURRENT.tables().stream()
            .map(table -> table.name())
            .filter(table -> !table.equals("scheduler_job"))
            .toList();
    List<MetadataRow> metadataRows = new ArrayList<>(tableRows(tables, catalog, null));
    metadataRows.add(new MetadataRow("neighbor_catalog", null, "scheduler_job", null));
    ResultSet otherCatalogTables = metadataRows(metadataRows);
    when(connection.getCatalog()).thenReturn(catalog);
    when(metadata.getTables(eq(catalog), any(), eq("%"), ArgumentMatchers.isNull()))
        .thenReturn(otherCatalogTables);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
  }

  private static List<String> schedulerJobColumnsWithoutPayload() {
    return RatchetSchemaCatalog.CURRENT.tables().stream()
        .filter(table -> table.name().equals("scheduler_job"))
        .flatMap(table -> table.columns().stream())
        .map(column -> column.name())
        .filter(column -> !column.equals("payload"))
        .toList();
  }

  @Test
  void validationRejectsMissingPrimaryKey() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    ResultSet noPrimaryKey = primaryKeyRows(List.of());
    when(metadata.getPrimaryKeys(
            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.eq("scheduler_job")))
        .thenReturn(noPrimaryKey);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
    assertTrue(ex.getMessage().contains("primary key"));
  }

  @Test
  void validationRejectsMissingIdempotencyUniqueIndex() throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    ResultSet noIdempotencyIndex = uniqueIndexRows("unused", List.of());
    when(metadata.getIndexInfo(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq("scheduler_job"),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(true)))
        .thenReturn(noIdempotencyIndex);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("scheduler_job"));
    assertTrue(ex.getMessage().contains("uk_idempotency_key"));
  }

  @Test
  void validationRejectsFutureMigrationHistory() throws Exception {
    configureCurrentSchemaMetadata(connection.getMetaData(), true);
    ResultSet history = historyRows(List.of(List.of("999", "checksum")));
    when(selectHistory.executeQuery()).thenReturn(history);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").validate());

    assertTrue(ex.getMessage().contains("newer than this runtime supports"));
  }

  @Test
  void validationRejectsAnIncompleteExistingMigrationLedger() throws Exception {
    configureCurrentSchemaMetadata(connection.getMetaData(), true);
    SchemaMigrator migrator = migrator("schema-migrator");
    SchemaMigrator.MigrationScript first = migrator.discoverMigrations().get(0);
    ResultSet history = historyRows(List.of(List.of(first.version(), first.checksum())));
    when(selectHistory.executeQuery()).thenReturn(history);

    SchemaMigrationException ex = assertThrows(SchemaMigrationException.class, migrator::validate);

    assertTrue(ex.getMessage().contains("missing recorded migration"));
    assertTrue(ex.getMessage().contains("002"));
    verify(statement, never()).execute(ArgumentMatchers.anyString());
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void failsWhenRecordedChecksumDoesNotMatchClasspathScript() throws Exception {
    ResultSet mismatchedVersion = existingVersion("not-the-current-checksum");
    when(selectVersion.executeQuery()).thenReturn(mismatchedVersion);

    assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void failsWhenRecordedChecksumIsMissing() throws Exception {
    ResultSet missingChecksum = existingVersion(" ");
    when(selectVersion.executeQuery()).thenReturn(missingChecksum);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    assertTrue(ex.getMessage().contains("already recorded without a checksum"));
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void failsWhenAdvisoryLockCannotBeAcquired() throws Exception {
    dialect.failAcquireWith(
        new SchemaMigrationException("Timed out acquiring schema migration lock"));

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    assertTrue(ex.getMessage().contains("Timed out acquiring schema migration lock"));
    verify(statement, never()).execute(startsWith("CREATE TABLE IF NOT EXISTS"));
    assertEquals(0, dialect.releaseCount());
  }

  @Test
  void rollsBackAndRestoresAutoCommitWhenMigrationStatementFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new SQLException("boom"));

    SQLException ex = assertThrows(SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("boom", ex.getMessage());
    verify(connection).setAutoCommit(false);
    verify(connection).rollback();
    verify(connection).setAutoCommit(true);
    verify(insertVersion, never()).executeUpdate();
    assertEquals(1, dialect.releaseCount());
  }

  @Test
  void keepsMigrationFailureWhenRollbackAlsoFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new SQLException("ddl failed"));
    doThrow(new SQLException("rollback failed")).when(connection).rollback();

    SQLException ex = assertThrows(SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("ddl failed", ex.getMessage());
    assertEquals(1, ex.getSuppressed().length);
    assertEquals("rollback failed", ex.getSuppressed()[0].getMessage());
    verify(connection).setAutoCommit(true);
  }

  @Test
  void surfacesReleaseLockFailureAfterMigrationWorkCompletes() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    ResultSet secondMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion, secondMissingVersion);
    dialect.failReleaseWith(new SQLException("Failed to release schema migration lock"));

    SQLException ex = assertThrows(SQLException.class, () -> migrator("schema-migrator").migrate());

    assertTrue(ex.getMessage().contains("Failed to release schema migration lock"));
    assertEquals(1, dialect.releaseCount());
  }

  @Test
  void keepsMigrationFailureWhenReleaseLockAlsoFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new SQLException("migration failed"));
    dialect.failReleaseWith(new SQLException("Failed to release schema migration lock"));

    SQLException ex = assertThrows(SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("migration failed", ex.getMessage());
    assertEquals(1, ex.getSuppressed().length);
    assertTrue(ex.getSuppressed()[0].getMessage().contains("Failed to release"));
    verify(connection).rollback();
  }

  @Test
  void lifecycleHookNamesExceptionWhenMigrationFailureHasNoMessage() throws Exception {
    @SuppressWarnings("unchecked")
    Instance<DataSource> dataSources = mock(Instance.class);
    DataSource failingDataSource = mock(DataSource.class);
    when(dataSources.isUnsatisfied()).thenReturn(false);
    when(dataSources.isAmbiguous()).thenReturn(false);
    when(dataSources.get()).thenReturn(failingDataSource);
    when(failingDataSource.getConnection()).thenThrow(new SQLException());

    SchemaMigrationDialect mysqlDialect = mock(SchemaMigrationDialect.class);
    when(mysqlDialect.id()).thenReturn("mysql");
    @SuppressWarnings("unchecked")
    Instance<SchemaMigrationDialect> dialects = mock(Instance.class);
    when(dialects.isUnsatisfied()).thenReturn(false);
    when(dialects.iterator()).thenReturn(List.of(mysqlDialect).iterator());

    RatchetOptions options =
        RatchetOptions.builder()
            .schema(
                schema ->
                    schema
                        .autoMigrate(true)
                        .migrationDialect("mysql")
                        .migrationPrefix("schema-migrator"))
            .build();
    SchemaMigrationLifecycleHook hook =
        new SchemaMigrationLifecycleHook(options, dataSources, dialects);

    SchemaInitializationException ex =
        assertThrows(SchemaInitializationException.class, hook::beforeStart);

    assertEquals("Ratchet schema auto-migration failed: SQLException", ex.getMessage());
  }

  /**
   * Minimal {@link SchemaMigrationDialect} for engine tests: returns predictable SQL and records
   * (or fails) lock calls so the engine's orchestration can be asserted without a real database.
   */
  private static final class RecordingDialect implements SchemaMigrationDialect {

    private RuntimeException acquireFailure;
    private SQLException releaseFailure;
    private int acquireCount;
    private int releaseCount;
    private boolean nativeBatch;

    void failAcquireWith(RuntimeException failure) {
      this.acquireFailure = failure;
    }

    void failReleaseWith(SQLException failure) {
      this.releaseFailure = failure;
    }

    int acquireCount() {
      return acquireCount;
    }

    int releaseCount() {
      return releaseCount;
    }

    @Override
    public String id() {
      return "stub";
    }

    @Override
    public String createVersionTableSql() {
      return "CREATE TABLE IF NOT EXISTS ratchet_schema_version (version VARCHAR(20) NOT NULL)";
    }

    @Override
    public String recordVersionSql() {
      return "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)";
    }

    @Override
    public boolean executesMigrationAsBatch() {
      return nativeBatch;
    }

    @Override
    public boolean usesDedicatedLockConnection() {
      return false;
    }

    @Override
    public void acquireLock(Connection connection) {
      acquireCount++;
      if (acquireFailure != null) {
        throw acquireFailure;
      }
    }

    @Override
    public void releaseLock(Connection connection) throws SQLException {
      releaseCount++;
      if (releaseFailure != null) {
        throw releaseFailure;
      }
    }
  }
}
