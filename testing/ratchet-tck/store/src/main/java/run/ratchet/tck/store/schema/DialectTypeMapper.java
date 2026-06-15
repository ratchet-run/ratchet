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
