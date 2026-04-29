package run.ratchet.tck.store.schema;

import java.util.List;
import java.util.Optional;

/**
 * Canonical index declaration. {@code partialPredicate} is present iff this is a partial index
 * (e.g. PostgreSQL {@code WHERE status='PENDING'}); dialects that lack partial-index support
 * satisfy the spec by carrying the full index without a predicate (the contract verifies the
 * predicate only when {@link DialectTypeMapper#supportsPartialIndexIntrospection()} is true).
 */
public record Index(
    String name,
    List<String> columns,
    boolean unique,
    Optional<LogicalPredicate> partialPredicate) {

  public static Index of(String name, String... columns) {
    return new Index(name, List.of(columns), false, Optional.empty());
  }

  public static Index unique(String name, String... columns) {
    return new Index(name, List.of(columns), true, Optional.empty());
  }

  public Index withPartialPredicate(LogicalPredicate predicate) {
    return new Index(name, columns, unique, Optional.of(predicate));
  }
}
