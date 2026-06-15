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
package run.ratchet.store.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import run.ratchet.tck.store.schema.DialectTypeMapper;
import run.ratchet.tck.store.schema.LogicalPredicate;
import run.ratchet.tck.store.schema.LogicalType;
import run.ratchet.tck.store.schema.OnDeleteAction;

/**
 * PostgreSQL type/action acceptance + partial-index predicate introspection for the schema
 * conformance contract. PostgreSQL's {@code information_schema} explicitly excludes PG-specific
 * features (per pgsql docs); partial-index WHERE predicates are read from {@code
 * pg_indexes.indexdef} with regex-extraction of the trailing {@code WHERE …} clause.
 */
final class PostgresqlDialectMapper implements DialectTypeMapper {

  /**
   * Normalize whitespace + outer parens emitted by {@code pg_get_indexdef()}. The function adds an
   * outer pair of parens around the entire predicate; we keep it because both renderPredicate() and
   * the introspected form will carry it. Multiple spaces collapse to one.
   */
  private static String normalize(String raw) {
    return raw.trim().replaceAll("\\s+", " ");
  }

  private static String textLiteral(String literal) {
    return "'" + literal.replace("'", "''") + "'::text";
  }

  private static void validateCommon(LogicalPredicate predicate) {
    Objects.requireNonNull(predicate, "predicate");
    if (predicate.column() == null || predicate.column().isBlank()) {
      throw new IllegalArgumentException("predicate column is required");
    }
    if (predicate.op() == null) {
      throw new IllegalArgumentException("predicate op is required");
    }
    if (predicate.literals() == null) {
      throw new IllegalArgumentException("predicate literals are required");
    }
    if (predicate.literals().stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("predicate literals must not contain null");
    }
  }

  private static String singleLiteral(LogicalPredicate predicate) {
    if (predicate.literals().size() != 1) {
      throw new IllegalArgumentException(
          predicate.op() + " predicate requires exactly one literal");
    }
    return predicate.literals().get(0);
  }

  private static List<String> nonEmptyLiterals(LogicalPredicate predicate) {
    if (predicate.literals().isEmpty()) {
      throw new IllegalArgumentException("IN predicate requires at least one literal");
    }
    return predicate.literals();
  }

  @Override
  public String dialectName() {
    return "PostgreSQL";
  }

  @Override
  public Set<String> acceptedTypes(LogicalType logical) {
    return switch (logical) {
      // PG's JDBC driver reports dialect names; both lower and capital forms appear depending on
      // the driver version, so we accept the canonical lowercase forms case-insensitively.
      case INT32 -> Set.of("int4", "integer", "int");
      case INT64 -> Set.of("int8", "bigint");
      case UUID -> Set.of("uuid");
      case TEXT -> Set.of("text", "varchar", "character varying", "bpchar");
      case CHAR_1 -> Set.of("bpchar", "character", "char");
      case TIMESTAMP_TZ -> Set.of("timestamptz", "timestamp with time zone");
      case BOOLEAN -> Set.of("bool", "boolean");
      case JSON -> Set.of("jsonb", "json");
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
  public boolean supportsPartialIndexIntrospection() {
    return true;
  }

  @Override
  public Optional<String> introspectIndexPredicate(
      Connection connection, String table, String indexName) throws SQLException {
    try (PreparedStatement ps =
        connection.prepareStatement(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
                + "AND tablename = ? AND indexname = ?")) {
      ps.setString(1, table);
      ps.setString(2, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        String indexdef = rs.getString(1);
        int wherePos = indexdef.toUpperCase().lastIndexOf(" WHERE ");
        if (wherePos < 0) {
          return Optional.empty();
        }
        return Optional.of(normalize(indexdef.substring(wherePos + " WHERE ".length())));
      }
    }
  }

  @Override
  public Optional<String> renderPredicate(LogicalPredicate predicate) {
    validateCommon(predicate);
    return switch (predicate.op()) {
      case EQ ->
          Optional.of(
              "(" + predicate.column() + " = " + textLiteral(singleLiteral(predicate)) + ")");
      case NEQ ->
          Optional.of(
              "(" + predicate.column() + " <> " + textLiteral(singleLiteral(predicate)) + ")");
      case IN ->
          Optional.of(
              "("
                  + predicate.column()
                  + " = ANY (ARRAY["
                  + nonEmptyLiterals(predicate).stream()
                      .map(PostgresqlDialectMapper::textLiteral)
                      .collect(Collectors.joining(", "))
                  + "]))");
    };
  }
}
