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
package run.ratchet.store.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import run.ratchet.tck.store.schema.DialectTypeMapper;
import run.ratchet.tck.store.schema.LogicalPredicate;
import run.ratchet.tck.store.schema.LogicalType;
import run.ratchet.tck.store.schema.OnDeleteAction;

/**
 * Oracle 23ai type/action acceptance for the schema conformance contract. Oracle has no
 * introspectable partial-index predicate (heap-table indexes are never partial); {@link
 * #supportsPartialIndexIntrospection()} stays {@code false} so the partial-predicate test skips,
 * and {@link #composeIndexColumns} prepends the predicate column to mirror the leading-column index
 * the Oracle DDL uses in place of a partial index.
 */
final class OracleDialectMapper implements DialectTypeMapper {

  @Override
  public String dialectName() {
    return "Oracle";
  }

  @Override
  public Set<String> acceptedTypes(LogicalType logical) {
    // Oracle reports DatabaseMetaData TYPE_NAME upper-case; NUMBER carries no INT/BIGINT
    // distinction
    // and TIMESTAMP includes its precision (e.g. "TIMESTAMP(6)"). JSON and large text are stored as
    // CLOB; bounded text is VARCHAR2.
    return switch (logical) {
      case INT32 -> Set.of("NUMBER");
      case INT64 -> Set.of("NUMBER");
      case UUID -> Set.of("RAW");
      case TEXT -> Set.of("VARCHAR2", "CLOB", "CHAR", "NCHAR", "NVARCHAR2");
      case CHAR_1 -> Set.of("CHAR");
      case TIMESTAMP_TZ -> Set.of("TIMESTAMP", "TIMESTAMP(6)", "TIMESTAMP(3)", "TIMESTAMP(0)");
      case BOOLEAN -> Set.of("BOOLEAN");
      case JSON -> Set.of("CLOB", "JSON");
    };
  }

  @Override
  public OnDeleteAction parseOnDelete(String introspectedValue) {
    if (introspectedValue == null) {
      return OnDeleteAction.NO_ACTION;
    }
    return switch (introspectedValue.toUpperCase()) {
      case "CASCADE" -> OnDeleteAction.CASCADE;
      case "RESTRICT" -> OnDeleteAction.RESTRICT;
      case "SET NULL" -> OnDeleteAction.SET_NULL;
      case "SET DEFAULT" -> OnDeleteAction.SET_DEFAULT;
      default -> OnDeleteAction.NO_ACTION;
    };
  }

  @Override
  public String metadataIdentifier(String name) {
    // Oracle stores unquoted identifiers upper-case; DatabaseMetaData lookups must match.
    return name == null ? null : name.toUpperCase(Locale.ROOT);
  }

  @Override
  public boolean nullabilityRelaxed(String table, String column) {
    // Oracle stores '' as NULL, so the engine's "" cron sentinel needs a nullable column.
    return "scheduler_job".equalsIgnoreCase(table) && "cron_expr".equalsIgnoreCase(column);
  }

  @Override
  public String deleteRuleQuery() {
    // Oracle has no information_schema; user_constraints carries delete_rule for R constraints.
    return "SELECT constraint_name AS constraint_name, delete_rule AS delete_rule"
        + " FROM user_constraints WHERE constraint_type = 'R' AND table_name = ?";
  }

  @Override
  public boolean supportsPartialIndexIntrospection() {
    return false;
  }

  /**
   * Oracle has no partial-index syntax. The canonical realization of a partial index in Oracle is
   * to prepend the predicate column to the index, leveraging leading-column equality for
   * selectivity. Skip the prepend if the column already appears in the canonical list.
   */
  @Override
  public List<String> composeIndexColumns(
      List<String> canonicalColumns, Optional<LogicalPredicate> partialPredicate) {
    if (partialPredicate.isEmpty()) {
      return canonicalColumns;
    }
    String predicateColumn = partialPredicate.get().column();
    if (canonicalColumns.contains(predicateColumn)) {
      return canonicalColumns;
    }
    List<String> composed = new ArrayList<>(canonicalColumns.size() + 1);
    composed.add(predicateColumn);
    composed.addAll(canonicalColumns);
    return composed;
  }
}
