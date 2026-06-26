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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.store.schema.Column;
import run.ratchet.tck.store.schema.DeprecatedArtifact;
import run.ratchet.tck.store.schema.DialectTypeMapper;
import run.ratchet.tck.store.schema.ForeignKey;
import run.ratchet.tck.store.schema.Index;
import run.ratchet.tck.store.schema.OnDeleteAction;
import run.ratchet.tck.store.schema.RatchetSchemaCatalog;
import run.ratchet.tck.store.schema.SchemaSpec;
import run.ratchet.tck.store.schema.Table;

/**
 * Schema conformance contract. Verifies that a conforming store's introspected schema satisfies the
 * canonical {@link RatchetSchemaCatalog}: required tables/columns/PKs/FKs/indexes are present and
 * conform; deprecated artifacts are absent (bidirectional check). Implementations supply a {@link
 * Connection} factory and a {@link DialectTypeMapper} that handles introspection asymmetries
 * (notably partial-index WHERE predicates).
 *
 * <p>The contract verifies presence and conformance, not exclusivity — a store with extra
 * dialect-private columns or indexes still passes. The bidirectional check applies only to
 * artifacts the catalog explicitly marks as removed.
 */
public abstract class AbstractSchemaConformanceContract {

  private String requireMetadataString(ResultSet rs, String metadataColumn, String subject)
      throws SQLException {
    String value = rs.getString(metadataColumn);
    if (value == null) {
      fail(
          mapper().dialectName()
              + " JDBC metadata column "
              + metadataColumn
              + " was null while introspecting "
              + subject);
    }
    return value;
  }

  @Test
  void allRequiredTablesPresent() throws SQLException {
    try (Connection c = openConnection()) {
      Set<String> introspected = introspectTableNames(c);
      for (Table t : expectedSchema().tables()) {
        assertTrue(
            introspected.contains(t.name().toLowerCase(Locale.ROOT)),
            () -> mapper().dialectName() + " missing required table: " + t.name());
      }
    }
  }

  @Test
  void allRequiredColumnsPresent_withAcceptableTypes() throws SQLException {
    try (Connection c = openConnection()) {
      for (Table t : expectedSchema().tables()) {
        Map<String, IntrospectedColumn> introspected = introspectColumns(c, t.name());
        for (Column expected : t.columns()) {
          IntrospectedColumn actual = introspected.get(expected.name().toLowerCase(Locale.ROOT));
          assertNotNull(
              actual,
              () -> mapper().dialectName() + " missing column " + t.name() + "." + expected.name());
          Set<String> accepted = mapper().acceptedTypes(expected.type());
          assertTrue(
              accepted.stream().anyMatch(s -> s.equalsIgnoreCase(actual.typeName())),
              () ->
                  mapper().dialectName()
                      + " column "
                      + t.name()
                      + "."
                      + expected.name()
                      + " has type "
                      + actual.typeName()
                      + "; expected one of "
                      + accepted);
          // A dialect may keep a catalog-NOT-NULL column nullable for a documented reason (Oracle
          // collapses '' to NULL, so cron_expr cannot be NOT NULL). It may never tighten a nullable
          // column to NOT NULL, so only this direction is tolerated.
          boolean relaxed =
              !expected.nullable()
                  && actual.nullable()
                  && mapper().nullabilityRelaxed(t.name(), expected.name());
          if (!relaxed) {
            assertEquals(
                expected.nullable(),
                actual.nullable(),
                () ->
                    mapper().dialectName()
                        + " column "
                        + t.name()
                        + "."
                        + expected.name()
                        + " nullability mismatch");
          }
        }
      }
    }
  }

  @Test
  void primaryKeysMatch() throws SQLException {
    try (Connection c = openConnection()) {
      DatabaseMetaData md = c.getMetaData();
      for (Table t : expectedSchema().tables()) {
        List<String> introspected = introspectPrimaryKey(md, t.name());
        assertEquals(
            t.primaryKey(),
            introspected,
            () -> mapper().dialectName() + " " + t.name() + " primary-key columns mismatch");
      }
    }
  }

  @Test
  void foreignKeysMatch_includingOnDeleteAction() throws SQLException {
    try (Connection c = openConnection()) {
      DatabaseMetaData md = c.getMetaData();
      for (Table t : expectedSchema().tables()) {
        Map<String, IntrospectedForeignKey> introspected = introspectForeignKeys(md, t.name());
        for (ForeignKey expected : t.foreignKeys()) {
          IntrospectedForeignKey actual =
              introspected.get(expected.column().toLowerCase(Locale.ROOT));
          assertNotNull(
              actual,
              () ->
                  mapper().dialectName() + " missing FK on " + t.name() + "." + expected.column());
          assertEquals(
              expected.refTable().toLowerCase(Locale.ROOT),
              actual.refTable().toLowerCase(Locale.ROOT),
              () -> "FK " + expected.name() + " referenced table mismatch");
          assertEquals(
              expected.refColumn().toLowerCase(Locale.ROOT),
              actual.refColumn().toLowerCase(Locale.ROOT),
              () -> "FK " + expected.name() + " referenced column mismatch");
          assertTrue(
              mapper().acceptsOnDelete(expected, actual.onDelete()),
              () ->
                  "FK "
                      + expected.name()
                      + " ON DELETE action mismatch: expected "
                      + expected.onDelete()
                      + " but introspected "
                      + actual.onDelete());
        }
      }
    }
  }

  @Test
  void nonPrimaryKeyIndexesPresent_withCorrectColumns() throws SQLException {
    try (Connection c = openConnection()) {
      DatabaseMetaData md = c.getMetaData();
      for (Table t : expectedSchema().tables()) {
        Map<String, IntrospectedIndex> introspected = introspectIndexes(md, t.name());
        for (Index expected : t.indexes()) {
          IntrospectedIndex actual = introspected.get(expected.name().toLowerCase(Locale.ROOT));
          assertNotNull(
              actual,
              () -> mapper().dialectName() + " missing index " + t.name() + "." + expected.name());
          List<String> expectedColumns =
              mapper().composeIndexColumns(expected.columns(), expected.partialPredicate());
          assertEquals(
              expectedColumns,
              actual.columns(),
              () -> "Index " + expected.name() + " column ordering mismatch");
          assertEquals(
              expected.unique(),
              actual.unique(),
              () -> "Index " + expected.name() + " uniqueness mismatch");
        }
      }
    }
  }

  @Test
  void partialIndexPredicatesMatch_whenIntrospectionSupported() throws SQLException {
    Assumptions.assumeTrue(
        mapper().supportsPartialIndexIntrospection(),
        () ->
            mapper().dialectName()
                + " does not support partial-index predicate introspection — skipping");
    try (Connection c = openConnection()) {
      for (Table t : expectedSchema().tables()) {
        for (Index idx : t.indexes()) {
          if (idx.partialPredicate().isEmpty()) {
            continue;
          }
          Optional<String> introspected =
              mapper().introspectIndexPredicate(c, t.name(), idx.name());
          Optional<String> rendered = mapper().renderPredicate(idx.partialPredicate().get());
          assertTrue(
              introspected.isPresent(),
              () -> "Partial predicate not introspected for index " + idx.name());
          assertTrue(
              rendered.isPresent(),
              () -> "Mapper failed to render expected predicate for " + idx.name());
          assertEquals(
              rendered.get().trim(),
              introspected.get().trim(),
              () -> "Partial-index predicate mismatch on " + idx.name());
        }
      }
    }
  }

  @Test
  void deprecatedColumnsAbsent() throws SQLException {
    try (Connection c = openConnection()) {
      List<String> violations = new ArrayList<>();
      Map<String, Map<String, IntrospectedColumn>> columnsByTable = new LinkedHashMap<>();
      for (DeprecatedArtifact artifact : expectedSchema().deprecated()) {
        if (artifact instanceof DeprecatedArtifact.DroppedColumn dc
            && expectedSchema().version() >= dc.sinceVersion()) {
          Map<String, IntrospectedColumn> cols = columnsByTable.get(dc.table());
          if (cols == null) {
            cols = introspectColumns(c, dc.table());
            columnsByTable.put(dc.table(), cols);
          }
          if (cols.containsKey(dc.column().toLowerCase(Locale.ROOT))) {
            violations.add(
                dc.table() + "." + dc.column() + " (dropped in v" + dc.sinceVersion() + ")");
          }
        }
      }
      if (!violations.isEmpty()) {
        fail(
            mapper().dialectName()
                + " carries columns the catalog marked as dropped: "
                + violations);
      }
    }
  }

  @Test
  void deprecatedIndexesAbsent() throws SQLException {
    try (Connection c = openConnection()) {
      DatabaseMetaData md = c.getMetaData();
      List<String> violations = new ArrayList<>();
      Map<String, Map<String, IntrospectedIndex>> indexesByTable = new LinkedHashMap<>();
      for (DeprecatedArtifact artifact : expectedSchema().deprecated()) {
        if (artifact instanceof DeprecatedArtifact.DroppedIndex di
            && expectedSchema().version() >= di.sinceVersion()) {
          Map<String, IntrospectedIndex> idxs = indexesByTable.get(di.table());
          if (idxs == null) {
            idxs = introspectIndexes(md, di.table());
            indexesByTable.put(di.table(), idxs);
          }
          if (idxs.containsKey(di.index().toLowerCase(Locale.ROOT))) {
            violations.add(
                di.table() + "." + di.index() + " (dropped in v" + di.sinceVersion() + ")");
          }
        }
      }
      if (!violations.isEmpty()) {
        fail(
            mapper().dialectName()
                + " carries indexes the catalog marked as dropped: "
                + violations);
      }
    }
  }

  /** Open a fresh JDBC connection to the conforming store under test. */
  protected abstract Connection openConnection() throws SQLException;

  /** Dialect mapper for type acceptance, FK action parsing, and partial-predicate introspection. */
  protected abstract DialectTypeMapper mapper();

  // -------- introspection helpers (JDBC standard) ----------

  /**
   * Default points at {@link RatchetSchemaCatalog#CURRENT}; subclasses may override for snapshots.
   */
  protected SchemaSpec expectedSchema() {
    return RatchetSchemaCatalog.CURRENT;
  }

  private Set<String> introspectTableNames(Connection c) throws SQLException {
    DatabaseMetaData md = c.getMetaData();
    Set<String> names = new HashSet<>();
    try (ResultSet rs = md.getTables(c.getCatalog(), c.getSchema(), "%", new String[] {"TABLE"})) {
      while (rs.next()) {
        names.add(requireMetadataString(rs, "TABLE_NAME", "table names").toLowerCase(Locale.ROOT));
      }
    }
    return names;
  }

  private Map<String, IntrospectedColumn> introspectColumns(Connection c, String table)
      throws SQLException {
    DatabaseMetaData md = c.getMetaData();
    Map<String, IntrospectedColumn> out = new LinkedHashMap<>();
    try (ResultSet rs =
        md.getColumns(c.getCatalog(), c.getSchema(), mapper().metadataIdentifier(table), "%")) {
      while (rs.next()) {
        String subject = "columns for table " + table;
        String name = requireMetadataString(rs, "COLUMN_NAME", subject).toLowerCase(Locale.ROOT);
        String type = requireMetadataString(rs, "TYPE_NAME", subject);
        boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
        out.put(name, new IntrospectedColumn(name, type, nullable));
      }
    }
    return out;
  }

  private List<String> introspectPrimaryKey(DatabaseMetaData md, String table) throws SQLException {
    Map<Short, String> ordered = new TreeMap<>();
    try (ResultSet rs =
        md.getPrimaryKeys(
            md.getConnection().getCatalog(),
            md.getConnection().getSchema(),
            mapper().metadataIdentifier(table))) {
      while (rs.next()) {
        ordered.put(
            rs.getShort("KEY_SEQ"),
            requireMetadataString(rs, "COLUMN_NAME", "primary key for table " + table)
                .toLowerCase(Locale.ROOT));
      }
    }
    return new ArrayList<>(ordered.values());
  }

  private Map<String, IntrospectedForeignKey> introspectForeignKeys(
      DatabaseMetaData md, String table) throws SQLException {
    Map<String, String> deleteRuleByConstraint = introspectDeleteRules(md.getConnection(), table);
    Map<String, IntrospectedForeignKey> out = new LinkedHashMap<>();
    try (ResultSet rs =
        md.getImportedKeys(
            md.getConnection().getCatalog(),
            md.getConnection().getSchema(),
            mapper().metadataIdentifier(table))) {
      while (rs.next()) {
        String subject = "imported foreign keys for table " + table;
        String fkColumn =
            requireMetadataString(rs, "FKCOLUMN_NAME", subject).toLowerCase(Locale.ROOT);
        String pkTable = requireMetadataString(rs, "PKTABLE_NAME", subject);
        String pkColumn = requireMetadataString(rs, "PKCOLUMN_NAME", subject);
        // Route the ON DELETE action through the dialect SPI: parseOnDelete normalizes the raw
        // information_schema delete_rule string (CASCADE / SET NULL / ...) into our enum. This is
        // the documented per-dialect parse path, so a non-conformant mapper is caught here rather
        // than silently bypassed by reading the JDBC DELETE_RULE short directly.
        String fkName = rs.getString("FK_NAME");
        OnDeleteAction action = mapper().parseOnDelete(deleteRuleByConstraint.get(fkName));
        out.put(fkColumn, new IntrospectedForeignKey(fkColumn, pkTable, pkColumn, action));
      }
    }
    return out;
  }

  /**
   * Reads each foreign key's ON DELETE rule as the dialect's raw {@code information_schema} {@code
   * delete_rule} string, keyed by constraint name. {@code referential_constraints} and {@code
   * key_column_usage} are SQL-standard, so this works on every SQL store without branching on
   * dialect; {@link DialectTypeMapper#parseOnDelete} then turns the string into the enum.
   */
  private Map<String, String> introspectDeleteRules(Connection connection, String table)
      throws SQLException {
    Map<String, String> byConstraint = new LinkedHashMap<>();
    String sql = mapper().deleteRuleQuery();
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, mapper().metadataIdentifier(table));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          byConstraint.put(rs.getString("constraint_name"), rs.getString("delete_rule"));
        }
      }
    }
    return byConstraint;
  }

  private Map<String, IntrospectedIndex> introspectIndexes(DatabaseMetaData md, String table)
      throws SQLException {
    Map<String, Map<Short, String>> columnsByIndex = new LinkedHashMap<>();
    Map<String, Boolean> uniqueByIndex = new LinkedHashMap<>();
    List<String> primaryKeyCols = introspectPrimaryKey(md, table);
    try (ResultSet rs =
        md.getIndexInfo(
            md.getConnection().getCatalog(),
            md.getConnection().getSchema(),
            mapper().metadataIdentifier(table),
            false,
            false)) {
      while (rs.next()) {
        String idxName = rs.getString("INDEX_NAME");
        if (idxName == null) {
          continue; // table-statistics row, not an index
        }
        // Skip the PK index — different dialects synthesize different names for it; PK is
        // verified separately. Heuristic: index whose columns exactly match the PK is the PK.
        String column = rs.getString("COLUMN_NAME");
        if (column == null) {
          continue;
        }
        boolean nonUnique = rs.getBoolean("NON_UNIQUE");
        short ordinal = rs.getShort("ORDINAL_POSITION");
        columnsByIndex
            .computeIfAbsent(idxName.toLowerCase(Locale.ROOT), k -> new TreeMap<>())
            .put(ordinal, column.toLowerCase(Locale.ROOT));
        uniqueByIndex.put(idxName.toLowerCase(Locale.ROOT), !nonUnique);
      }
    }
    Map<String, IntrospectedIndex> out = new LinkedHashMap<>();
    columnsByIndex.forEach(
        (name, ordered) -> {
          List<String> cols = new ArrayList<>(ordered.values());
          if (cols.equals(primaryKeyCols)) {
            return; // skip PK index — verified separately
          }
          out.put(name, new IntrospectedIndex(name, cols, uniqueByIndex.getOrDefault(name, false)));
        });
    return out;
  }

  // ---- introspected-state value carriers ----

  private record IntrospectedColumn(String name, String typeName, boolean nullable) {}

  private record IntrospectedForeignKey(
      String column, String refTable, String refColumn, OnDeleteAction onDelete) {}

  private record IntrospectedIndex(String name, List<String> columns, boolean unique) {}
}
