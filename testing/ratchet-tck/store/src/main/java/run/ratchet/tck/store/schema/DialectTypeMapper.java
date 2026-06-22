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
package run.ratchet.tck.store.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Dialect-specific lens onto an introspected schema. Implementations encapsulate every place where
 * the conformance contract would otherwise need to branch on store identity: type acceptance, FK
 * action wire-format, partial-index predicate introspection.
 *
 * <p>Implementations live in each store module's test sources alongside the contract subclass.
 */
public interface DialectTypeMapper {

  /** Human-readable dialect name for assertion messages only. Not used for branching. */
  String dialectName();

  /** Set of dialect-specific type names that satisfy a logical type. */
  Set<String> acceptedTypes(LogicalType logical);

  /**
   * Translate a raw {@code information_schema} ON DELETE value into our enum. MySQL emits {@code NO
   * ACTION}; PostgreSQL emits the same; some dialects emit {@code RESTRICT} or {@code 'a'}.
   */
  OnDeleteAction parseOnDelete(String introspectedValue);

  /**
   * Whether the introspected {@code actual} ON DELETE action satisfies the catalog's expectation
   * for {@code expected}. Defaults to strict equality. A dialect overrides this only where it
   * cannot express a cataloged action — e.g. SQL Server forbids two {@code ON DELETE CASCADE}
   * foreign keys from one table to the same parent, so it substitutes {@code NO_ACTION} and deletes
   * the dependent rows in application code.
   */
  default boolean acceptsOnDelete(ForeignKey expected, OnDeleteAction actual) {
    return expected.onDelete() == actual;
  }

  /**
   * Whether this dialect can introspect partial-index WHERE predicates. {@code false} when the
   * dialect lacks the concept (MySQL covering indexes) or lacks an introspection path. Tests that
   * verify partial predicates skip when this returns {@code false}.
   */
  default boolean supportsPartialIndexIntrospection() {
    return false;
  }

  /**
   * Introspect the partial-index WHERE predicate for the given index. Required when {@link
   * #supportsPartialIndexIntrospection()} is true; default implementation returns empty for
   * dialects where it is false. Implementations MUST normalize predicate text before returning
   * (whitespace, quoting) so that string equality is meaningful.
   */
  default Optional<String> introspectIndexPredicate(
      Connection connection, String table, String indexName) throws SQLException {
    return Optional.empty();
  }

  /** Render a logical predicate to the canonical normalized form this dialect would emit. */
  default Optional<String> renderPredicate(LogicalPredicate predicate) {
    return Optional.empty();
  }

  /**
   * Normalize a catalog table/identifier for {@link java.sql.DatabaseMetaData} lookups. MySQL and
   * PostgreSQL match the catalog's lower-case names as stored, so the default returns the name
   * unchanged; Oracle stores unquoted identifiers upper-case and must override to match.
   */
  default String metadataIdentifier(String name) {
    return name;
  }

  /**
   * Whether this dialect is permitted to leave a column nullable even though the catalog declares
   * it NOT NULL. Only relaxation (NOT NULL → nullable) is allowed, for documented dialect reasons —
   * e.g. Oracle collapses the empty string to NULL, so {@code scheduler_job.cron_expr} (whose
   * engine sentinel is {@code ""}) cannot be NOT NULL. Defaults to {@code false}.
   */
  default boolean nullabilityRelaxed(String table, String column) {
    return false;
  }

  /**
   * SQL that reads each foreign key's ON DELETE rule for a table, keyed by constraint name, with a
   * single {@code ?} bind for the (already {@link #metadataIdentifier normalized}) table name and
   * result columns aliased {@code constraint_name} and {@code delete_rule}. The default uses the
   * SQL-standard {@code information_schema}; Oracle, which has no {@code information_schema},
   * overrides with {@code user_constraints}.
   */
  default String deleteRuleQuery() {
    return "SELECT rc.constraint_name AS constraint_name, rc.delete_rule AS delete_rule"
        + " FROM information_schema.referential_constraints rc"
        + " JOIN information_schema.key_column_usage kcu"
        + "   ON rc.constraint_name = kcu.constraint_name"
        + "   AND rc.constraint_schema = kcu.constraint_schema"
        + " WHERE kcu.table_name = ?";
  }

  /**
   * Compose the dialect-specific index column ordering from the catalog's canonical columns and
   * (optional) partial-index predicate. Dialects that support partial indexes (PostgreSQL) return
   * {@code canonicalColumns} unchanged; dialects without partial-index support (MySQL covering
   * indexes) typically prepend the predicate column for selectivity. Implementations should not
   * duplicate a column that already appears in {@code canonicalColumns}.
   */
  default List<String> composeIndexColumns(
      List<String> canonicalColumns, Optional<LogicalPredicate> partialPredicate) {
    return canonicalColumns;
  }
}
