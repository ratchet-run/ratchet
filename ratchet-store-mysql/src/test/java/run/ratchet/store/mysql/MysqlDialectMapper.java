package run.ratchet.store.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import run.ratchet.tck.store.schema.DialectTypeMapper;
import run.ratchet.tck.store.schema.LogicalPredicate;
import run.ratchet.tck.store.schema.LogicalType;
import run.ratchet.tck.store.schema.OnDeleteAction;

/**
 * MySQL 8 type/action acceptance for the schema conformance contract. MySQL has no introspectable
 * partial-index predicate (covering indexes are not partial); {@link
 * #supportsPartialIndexIntrospection()} stays {@code false} so the partial-predicate test skips.
 */
final class MysqlDialectMapper implements DialectTypeMapper {

  @Override
  public String dialectName() {
    return "MySQL";
  }

  @Override
  public Set<String> acceptedTypes(LogicalType logical) {
    return switch (logical) {
      case INT32 -> Set.of("INT", "INT UNSIGNED", "TINYINT", "TINYINT UNSIGNED", "SMALLINT");
      case INT64 -> Set.of("BIGINT", "BIGINT UNSIGNED");
      case UUID -> Set.of("BINARY"); // BINARY(16); column-length not policed here
      // MySQL 8 reports ENUM-typed columns as "ENUM" and free-text columns as TEXT/VARCHAR; both
      // satisfy LogicalType.TEXT because the catalog does not police domain-of-values here.
      case TEXT -> Set.of("TEXT", "MEDIUMTEXT", "LONGTEXT", "VARCHAR", "ENUM", "CHAR");
      case CHAR_1 -> Set.of("CHAR");
      case TIMESTAMP_TZ -> Set.of("DATETIME", "TIMESTAMP");
      case BOOLEAN -> Set.of("TINYINT", "BIT", "BOOLEAN");
      case JSON -> Set.of("JSON", "LONGTEXT");
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
    return false;
  }

  /**
   * MySQL has no partial-index syntax. The canonical realization of a partial index in MySQL is to
   * prepend the predicate column to the index, leveraging leading-column equality for selectivity.
   * Skip the prepend if the column already appears in the canonical list.
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
