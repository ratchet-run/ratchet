package run.ratchet.tck.store.schema;

import java.util.List;

/**
 * Dialect-agnostic predicate for partial-index WHERE clauses (and, prospectively, CHECK
 * constraints). The {@link DialectTypeMapper} renders predicates into dialect SQL and parses
 * introspected predicates back into this form for comparison.
 */
public record LogicalPredicate(String column, Op op, List<String> literals) {

  public enum Op {
    EQ,
    NEQ,
    IN
  }

  public static LogicalPredicate eq(String column, String literal) {
    return new LogicalPredicate(column, Op.EQ, List.of(literal));
  }

  public static LogicalPredicate in(String column, String... literals) {
    return new LogicalPredicate(column, Op.IN, List.of(literals));
  }
}
