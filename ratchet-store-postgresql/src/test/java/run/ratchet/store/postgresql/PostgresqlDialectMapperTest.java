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
