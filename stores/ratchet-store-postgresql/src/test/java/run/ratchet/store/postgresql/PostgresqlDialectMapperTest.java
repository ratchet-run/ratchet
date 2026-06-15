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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.store.schema.LogicalPredicate;

class PostgresqlDialectMapperTest {

  private final PostgresqlDialectMapper mapper = new PostgresqlDialectMapper();

  @Test
  void renderPredicateEscapesEqLiteral() {
    assertEquals(
        "(status = 'PENDING''; DROP INDEX idx_claim_executable; --'::text)",
        mapper
            .renderPredicate(
                LogicalPredicate.eq("status", "PENDING'; DROP INDEX idx_claim_executable; --"))
            .orElseThrow());
  }

  @Test
  void renderPredicateEscapesInLiterals() {
    assertEquals(
        "(status = ANY (ARRAY['PENDING'::text, 'needs ''quote'''::text, 'semi;--'::text]))",
        mapper
            .renderPredicate(LogicalPredicate.in("status", "PENDING", "needs 'quote'", "semi;--"))
            .orElseThrow());
  }

  @Test
  void renderPredicateEscapesNeqLiteral() {
    assertEquals(
        "(status <> 'not ''done'''::text)",
        mapper.renderPredicate(LogicalPredicate.neq("status", "not 'done'")).orElseThrow());
  }

  @Test
  void renderPredicateRejectsMissingEqLiteral() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mapper.renderPredicate(
                    new LogicalPredicate("status", LogicalPredicate.Op.EQ, List.of())));

    assertTrue(thrown.getMessage().contains("exactly one literal"));
  }

  @Test
  void renderPredicateRejectsEmptyInLiteralList() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mapper.renderPredicate(
                    new LogicalPredicate("status", LogicalPredicate.Op.IN, List.of())));

    assertTrue(thrown.getMessage().contains("at least one literal"));
  }

  @Test
  void renderPredicateRejectsNullLiteral() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mapper.renderPredicate(
                    new LogicalPredicate(
                        "status", LogicalPredicate.Op.NEQ, Arrays.asList((String) null))));

    assertTrue(thrown.getMessage().contains("must not contain null"));
  }
}
