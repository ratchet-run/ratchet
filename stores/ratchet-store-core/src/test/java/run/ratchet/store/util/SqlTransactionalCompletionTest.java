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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.spi.TerminalOutcome;

class SqlTransactionalCompletionTest {
  @Test
  void outcomeTableLocksHotThenColdForBothSqlForms() {
    // A cold row without terminal status and without a hot row violates the store invariant and
    // cannot be created through the public API. Exercise that defensive case here too.
    var cases =
        List.of(
            new OutcomeCase(
                new Object[] {"RUNNING", 7L}, new Object[] {null, 0L}, TerminalOutcome.ROLLED_BACK),
            new OutcomeCase(
                new Object[] {"RUNNING", 8L}, new Object[] {null, 0L}, TerminalOutcome.SUPERSEDED),
            new OutcomeCase(
                new Object[] {"PENDING", 7L}, new Object[] {null, 0L}, TerminalOutcome.SUPERSEDED),
            new OutcomeCase(
                new Object[] {"WAITING", 7L},
                new Object[] {"SUCCEEDED", 7L},
                TerminalOutcome.SUPERSEDED),
            new OutcomeCase(null, new Object[] {"SUCCEEDED", 7L}, TerminalOutcome.COMMITTED),
            new OutcomeCase(null, new Object[] {"SUCCEEDED", 8L}, TerminalOutcome.SUPERSEDED),
            new OutcomeCase(null, new Object[] {"CANCELED", 0L}, TerminalOutcome.CANCELED),
            new OutcomeCase(null, new Object[] {"FAILED", 7L}, TerminalOutcome.SUPERSEDED),
            new OutcomeCase(null, new Object[] {null, 7L}, TerminalOutcome.SUPERSEDED),
            new OutcomeCase(null, null, TerminalOutcome.SUPERSEDED));
    for (boolean sqlServer : List.of(false, true)) {
      for (OutcomeCase test : cases) {
        EntityManager em = mock(EntityManager.class);
        Query hot = mock(Query.class, RETURNS_SELF);
        Query cold = mock(Query.class, RETURNS_SELF);
        String hint = sqlServer ? " WITH (UPDLOCK, ROWLOCK)" : "";
        String suffix = sqlServer ? "" : " FOR UPDATE";
        String hotSql =
            "SELECT status, claim_seq FROM scheduler_job_queue"
                + hint
                + " WHERE job_id = ?"
                + suffix;
        String coldSql =
            "SELECT terminal_status, claim_seq FROM scheduler_job"
                + hint
                + " WHERE job_id = ?"
                + suffix;
        when(em.createNativeQuery(hotSql)).thenReturn(hot);
        when(em.createNativeQuery(coldSql)).thenReturn(cold);
        when(hot.getResultList()).thenReturn(rows(test.hot()));
        when(cold.getResultList()).thenReturn(rows(test.cold()));
        UUID id = UUID.randomUUID();
        assertEquals(
            test.outcome(),
            SqlJobCompletion.terminalOutcome(em, id, 7L, sqlServer, value -> value));
        var order = inOrder(em, hot, cold);
        order.verify(em).createNativeQuery(hotSql);
        order.verify(hot).setParameter(1, id);
        order.verify(hot).getResultList();
        order.verify(em).createNativeQuery(coldSql);
        order.verify(cold).setParameter(1, id);
        order.verify(cold).getResultList();
      }
    }
  }

  private static List<Object[]> rows(Object[] row) {
    return row == null ? List.of() : List.<Object[]>of(row);
  }

  private record OutcomeCase(Object[] hot, Object[] cold, TerminalOutcome outcome) {}
}
