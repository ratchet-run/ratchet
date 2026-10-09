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
package run.ratchet.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.ApplicationContext;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.exceptions.IllegalTransactionStateException;
import jakarta.transaction.Transactional.TxType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.micronaut.internal.NoTransactionOperations;

class MicronautTransactionsTest {
  private ApplicationContext context;
  private TransactionOperations<?> operations;

  @BeforeEach
  void start() {
    context =
        TestContexts.core(
            Map.of(
                "ratchet.enabled", false,
                "datasources.default.url",
                    "jdbc:h2:mem:transactions-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
                "datasources.default.driver-class-name", "org.h2.Driver",
                "datasources.default.username", "sa",
                "datasources.default.password", ""));
    context.start();
    operations = context.getBean(TransactionOperations.class);
  }

  @AfterEach
  void close() {
    if (context != null) context.close();
  }

  @Test
  void requiredInsideNotSupportedCommitsSeparatelyFromSuspendedTransaction() {
    operations.execute(
        TransactionDefinition.of(Propagation.REQUIRED),
        outer -> {
          execute(outer, "create table helper_work (id integer)");
          execute(outer, "insert into helper_work values (1)");
          operations.execute(
              TransactionDefinition.of(Propagation.NOT_SUPPORTED),
              inert -> {
                assertFalse(MicronautTransactions.hasActiveTransaction(operations));
                MicronautTransactions.execute(
                    operations,
                    TxType.REQUIRED,
                    inner -> {
                      assertTrue(inner.isNewTransaction());
                      assertTrue(MicronautTransactions.hasActiveTransaction(operations));
                      assertNotSame(outer.getConnection(), inner.getConnection());
                      assertNotSame(inert.getConnection(), inner.getConnection());
                      assertTransactional(inner);
                      execute(inner, "insert into helper_work values (2)");
                      return null;
                    });
                assertFalse(MicronautTransactions.hasActiveTransaction(operations));
                return null;
              });
          outer.setRollbackOnly();
          return null;
        });
    MicronautTransactions.execute(
        operations,
        TxType.REQUIRED,
        status -> {
          try (var statement = ((Connection) status.getConnection()).createStatement();
              var rows = statement.executeQuery("select id from helper_work")) {
            assertTrue(rows.next());
            assertEquals(2, rows.getInt(1));
            assertFalse(rows.next());
          } catch (SQLException failure) {
            throw new IllegalStateException(failure);
          }
          return null;
        });
  }

  @Test
  void requiredInsideSupportsWithoutTransactionStartsRealTransaction() {
    operations.execute(
        TransactionDefinition.of(Propagation.SUPPORTS),
        inert -> {
          assertFalse(MicronautTransactions.hasActiveTransaction(operations));
          MicronautTransactions.execute(
              operations,
              TxType.REQUIRED,
              inner -> {
                assertTrue(inner.isNewTransaction());
                assertTrue(MicronautTransactions.hasActiveTransaction(operations));
                assertTransactional(inner);
                return null;
              });
          assertFalse(MicronautTransactions.hasActiveTransaction(operations));
          return null;
        });
  }

  @Test
  void requiredInsideRealTransactionJoinsAndExistingChainsShareRoot() {
    MicronautTransactions.execute(
        operations,
        TxType.REQUIRED,
        outer -> {
          MicronautTransactions.execute(
              operations,
              TxType.REQUIRED,
              inner -> {
                assertFalse(inner.isNewTransaction());
                assertSame(outer, MicronautTransactions.rootStatus(inner));
                assertSame(outer.getConnection(), inner.getConnection());
                MicronautTransactions.execute(
                    operations,
                    TxType.REQUIRED,
                    nested -> {
                      assertSame(outer, MicronautTransactions.rootStatus(nested));
                      assertTrue(MicronautTransactions.hasActiveTransaction(operations));
                      return null;
                    });
                return null;
              });
          return null;
        });
  }

  @Test
  void mandatoryInsideNotSupportedRejectsMissingTransaction() {
    operations.execute(
        TransactionDefinition.of(Propagation.NOT_SUPPORTED),
        status -> {
          IllegalTransactionStateException failure =
              assertThrows(
                  IllegalTransactionStateException.class,
                  () ->
                      MicronautTransactions.execute(
                          operations, TxType.MANDATORY, ignored -> fail("must not execute")));
          assertEquals(
              "No existing transaction found for transaction marked with propagation 'mandatory'",
              failure.getMessage());
          return null;
        });
  }

  @Test
  void inertOperationsRunMandatoryWorkWithoutATransactionManager() {
    var inert = new NoTransactionOperations();
    assertEquals("ran", MicronautTransactions.execute(inert, TxType.MANDATORY, ignored -> "ran"));
    assertEquals(
        Propagation.REQUIRED, MicronautTransactions.effectivePropagation(inert, TxType.REQUIRED));
  }

  @Test
  void existingChainsRootedInNoTxAreNotActive() {
    operations.execute(
        TransactionDefinition.of(Propagation.NOT_SUPPORTED),
        root -> {
          operations.execute(
              TransactionDefinition.of(Propagation.REQUIRED),
              inner -> {
                operations.execute(
                    TransactionDefinition.of(Propagation.REQUIRED),
                    nested -> {
                      assertNotSame(inner, nested);
                      assertSame(root, MicronautTransactions.rootStatus(nested));
                      assertFalse(MicronautTransactions.hasActiveTransaction(operations));
                      return null;
                    });
                return null;
              });
          return null;
        });
  }

  @Test
  void missingAndCompletedStatusesAreInactiveAndOtherManagersAreActive() {
    assertFalse(MicronautTransactions.hasActiveTransaction(operations));
    TransactionOperations<?> other = mock(TransactionOperations.class);
    TransactionStatus<?> status = mock(TransactionStatus.class);
    doReturn(Optional.of(status)).when(other).findTransactionStatus();
    assertTrue(MicronautTransactions.hasActiveTransaction(other));
    when(status.isCompleted()).thenReturn(true);
    assertFalse(MicronautTransactions.hasActiveTransaction(other));
  }

  private static void assertTransactional(TransactionStatus<?> status) {
    try {
      assertFalse(((Connection) status.getConnection()).getAutoCommit());
    } catch (SQLException failure) {
      throw new IllegalStateException(failure);
    }
  }

  private static void execute(TransactionStatus<?> status, String sql) {
    try (var statement = ((Connection) status.getConnection()).createStatement()) {
      statement.execute(sql);
    } catch (SQLException failure) {
      throw new IllegalStateException(failure);
    }
  }
}
