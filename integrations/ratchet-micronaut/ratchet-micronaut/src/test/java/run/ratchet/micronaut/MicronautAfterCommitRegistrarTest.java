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
import io.micronaut.transaction.support.TransactionSynchronization;
import jakarta.transaction.Transactional.TxType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.micronaut.internal.NoTransactionOperations;
import run.ratchet.spi.AfterCommitRegistrar.Result;

class MicronautAfterCommitRegistrarTest {
  private ApplicationContext context;
  private TransactionOperations<?> manager;
  private MicronautAfterCommitRegistrar registrar;

  @BeforeEach
  void start() {
    context =
        TestContexts.core(
            Map.of(
                "ratchet.enabled",
                false,
                "datasources.default.url",
                "jdbc:h2:mem:after-commit-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
                "datasources.default.driver-class-name",
                "org.h2.Driver",
                "datasources.default.username",
                "sa",
                "datasources.default.password",
                ""));
    context.start();
    manager = context.getBean(TransactionOperations.class);
    registrar = new MicronautAfterCommitRegistrar(() -> manager);
    assertFalse(manager instanceof NoTransactionOperations);
  }

  @AfterEach
  void close() {
    if (context != null) context.close();
  }

  private void transaction(Propagation propagation, Consumer<TransactionStatus<?>> action) {
    MicronautTransactions.execute(
        manager,
        TxType.valueOf(propagation.name()),
        status -> {
          action.accept(status);
          return null;
        });
  }

  @Test
  void fifoContinuesAfterFailureAndCallbacksHaveNoAmbientTransaction() {
    List<String> calls = new ArrayList<>();
    transaction(
        Propagation.REQUIRED,
        status -> {
          assertEquals(
              Result.REGISTERED,
              registrar.registerAfterCommit(
                  () -> {
                    assertEquals(
                        Result.NO_ACTIVE_TRANSACTION,
                        registrar.registerAfterCommit(() -> fail("not registered")));
                    calls.add("first");
                  }));
          registrar.registerAfterCommit(
              () -> {
                throw new IllegalStateException("observer");
              });
          registrar.registerAfterCommit(() -> calls.add("last"));
        });
    assertEquals(List.of("first", "last"), calls);
  }

  @Test
  void requiresNewHasItsOwnQueueAndOuterRollbackSuppressesItsCallbacks() {
    List<String> calls = new ArrayList<>();
    transaction(
        Propagation.REQUIRED,
        status -> {
          registrar.registerAfterCommit(() -> calls.add("outer"));
          transaction(
              Propagation.REQUIRES_NEW,
              inner -> registrar.registerAfterCommit(() -> calls.add("inner")));
          status.setRollbackOnly();
        });
    assertEquals(List.of("inner"), calls);
  }

  @Test
  void restoresOuterQueueAndRequiredParticipationKeepsFifo() {
    List<String> calls = new ArrayList<>();
    transaction(
        Propagation.REQUIRED,
        status -> {
          registrar.registerAfterCommit(() -> calls.add("outer-first"));
          transaction(
              Propagation.REQUIRES_NEW,
              inner -> registrar.registerAfterCommit(() -> calls.add("inner")));
          transaction(
              Propagation.REQUIRED,
              inner -> registrar.registerAfterCommit(() -> calls.add("outer-middle")));
          registrar.registerAfterCommit(() -> calls.add("outer-last"));
        });
    assertEquals(List.of("inner", "outer-first", "outer-middle", "outer-last"), calls);
    assertEquals(Result.NO_ACTIVE_TRANSACTION, registrar.registerAfterCommit(() -> {}));
  }

  @Test
  void notSupportedRunsImmediatelyWhileOuterRollsBack() {
    transaction(
        Propagation.REQUIRED,
        status -> {
          transaction(
              Propagation.NOT_SUPPORTED,
              ignored ->
                  assertEquals(
                      Result.NO_ACTIVE_TRANSACTION, registrar.registerAfterCommit(() -> {})));
          status.setRollbackOnly();
        });
  }

  @Test
  void rawRequiredInsideNotSupportedCannotRegisterAfterCommit() {
    transaction(
        Propagation.NOT_SUPPORTED,
        ignored ->
            manager.execute(
                TransactionDefinition.of(Propagation.REQUIRED),
                status -> {
                  assertEquals(
                      Result.NO_ACTIVE_TRANSACTION,
                      registrar.registerAfterCommit(() -> fail("no transaction")));
                  return null;
                }));
  }

  @Test
  void sequentialTransactionsAndNestedCallbackTransactionsRemainSeparate() {
    List<String> calls = new ArrayList<>();
    transaction(
        Propagation.REQUIRED,
        status ->
            registrar.registerAfterCommit(
                () -> {
                  calls.add("first");
                  transaction(
                      Propagation.REQUIRED,
                      inner -> registrar.registerAfterCommit(() -> calls.add("nested")));
                }));
    transaction(
        Propagation.REQUIRED, status -> registrar.registerAfterCommit(() -> calls.add("last")));
    assertEquals(List.of("first", "nested", "last"), calls);
  }

  @Test
  void requiredCallbackWorkCommitsIndependently() {
    transaction(
        Propagation.REQUIRED,
        status -> {
          execute(status, "create table callback_work (id integer primary key)");
          execute(status, "insert into callback_work values (1)");
          registrar.registerAfterCommit(
              () ->
                  transaction(
                      Propagation.REQUIRED,
                      inner -> {
                        execute(inner, "insert into callback_work values (2)");
                        registrar.registerAfterCommit(
                            () ->
                                transaction(
                                    Propagation.REQUIRED, check -> assertEquals(2, count(check))));
                      }));
        });
    transaction(Propagation.REQUIRED, status -> assertEquals(2, count(status)));
    transaction(
        Propagation.REQUIRED,
        status -> {
          registrar.registerAfterCommit(
              () ->
                  transaction(
                      Propagation.REQUIRED,
                      inner -> execute(inner, "insert into callback_work values (3)")));
          status.setRollbackOnly();
        });
    transaction(Propagation.REQUIRED, status -> assertEquals(2, count(status)));
  }

  @Test
  void distinguishesMissingTransactionAndRegistrationFailureAndRejectsAmbiguousManager() {
    assertEquals(Result.NO_ACTIVE_TRANSACTION, registrar.registerAfterCommit(() -> {}));
    TransactionOperations<?> invalidManager = mock(TransactionOperations.class);
    TransactionStatus<?> active = mock(TransactionStatus.class);
    when(active.getTransaction()).thenReturn(new Object());
    doReturn(Optional.of(active)).when(invalidManager).findTransactionStatus();
    doThrow(new IllegalStateException("no synchronization"))
        .when(active)
        .registerSynchronization(any(TransactionSynchronization.class));
    assertEquals(
        Result.REGISTRATION_FAILED,
        new MicronautAfterCommitRegistrar(() -> invalidManager).registerAfterCommit(() -> {}));
    var ambiguous =
        new MicronautAfterCommitRegistrar(
            () -> {
              throw new IllegalStateException("ambiguous");
            });
    assertTrue(
        assertThrows(
                IllegalStateException.class,
                () ->
                    transaction(
                        Propagation.REQUIRED, status -> ambiguous.registerAfterCommit(() -> {})))
            .getMessage()
            .contains("default or @Primary"));
    assertThrows(NullPointerException.class, () -> registrar.registerAfterCommit(null));
  }

  private static void execute(TransactionStatus<?> status, String sql) {
    // Micronaut 4 exposes a wildcard connection; retain the cast for the 4/5 source contract.
    try (var statement = ((Connection) status.getConnection()).createStatement()) {
      statement.execute(sql);
    } catch (SQLException failure) {
      throw new IllegalStateException(failure);
    }
  }

  private static int count(TransactionStatus<?> status) {
    try (var statement = ((Connection) status.getConnection()).createStatement();
        var rows = statement.executeQuery("select count(*) from callback_work")) {
      assertTrue(rows.next());
      return rows.getInt(1);
    } catch (SQLException failure) {
      throw new IllegalStateException(failure);
    }
  }
}
