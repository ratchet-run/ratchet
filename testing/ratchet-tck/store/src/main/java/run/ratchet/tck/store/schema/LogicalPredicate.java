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

import java.util.List;

/**
 * Dialect-agnostic predicate for partial-index WHERE clauses (and, prospectively, CHECK
 * constraints). The {@link DialectTypeMapper} renders predicates into dialect SQL and parses
 * introspected predicates back into this form for comparison.
 */
public record LogicalPredicate(String column, Op op, List<String> literals) {

  public static LogicalPredicate eq(String column, String literal) {
    return new LogicalPredicate(column, Op.EQ, List.of(literal));
  }

  public static LogicalPredicate neq(String column, String literal) {
    return new LogicalPredicate(column, Op.NEQ, List.of(literal));
  }

  public static LogicalPredicate in(String column, String... literals) {
    return new LogicalPredicate(column, Op.IN, List.of(literals));
  }

  public enum Op {
    EQ,
    NEQ,
    IN
  }
}
