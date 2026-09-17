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
package run.ratchet.spring.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.spi.AfterCommitRegistrar.Result;

class SpringAfterCommitRegistrarTest {
  private final DataSourceTransactionManager manager = transactionManager();
  private final SpringAfterCommitRegistrar registrar =
      new SpringAfterCommitRegistrar(() -> manager);

  @AfterEach
  void clearTransactionState() {
    TransactionSynchronizationManager.clear();
  }

  @Test
  void preservesFifoAndContinuesAfterOneCallbackFails() {
    List<String> calls = new ArrayList<>();
    new TransactionTemplate(manager)
        .executeWithoutResult(
            status -> {
              assertEquals(
                  Result.REGISTERED,
                  registrar.registerAfterCommit(
                      () -> {
                        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
                        assertFalse(
                            TransactionSynchronizationManager.hasResource(manager.getDataSource()));
                        calls.add("first");
                      }));
              assertEquals(
                  Result.REGISTERED,
                  registrar.registerAfterCommit(
                      () -> {
                        throw new IllegalStateException("observer");
                      }));
              assertEquals(
                  Result.REGISTERED, registrar.registerAfterCommit(() -> calls.add("last")));
            });

    assertEquals(List.of("first", "last"), calls);
  }

  @Test
  void distinguishesNoTransactionFromAnActiveTransactionWithoutSynchronization() {
    assertEquals(Result.NO_ACTIVE_TRANSACTION, registrar.registerAfterCommit(() -> {}));

    TransactionSynchronizationManager.setActualTransactionActive(true);

    assertEquals(
        Result.ACTIVE_TRANSACTION_REGISTRATION_FAILED, registrar.registerAfterCommit(() -> {}));
  }

  @Test
  void requiresNewHasItsOwnQueueWhenOuterTransactionRollsBack() {
    TransactionTemplate outer = new TransactionTemplate(manager);
    TransactionTemplate inner = new TransactionTemplate(manager);
    inner.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    List<String> calls = new ArrayList<>();

    outer.executeWithoutResult(
        ignored -> {
          assertEquals(Result.REGISTERED, registrar.registerAfterCommit(() -> calls.add("outer")));
          inner.executeWithoutResult(
              alsoIgnored ->
                  assertEquals(
                      Result.REGISTERED, registrar.registerAfterCommit(() -> calls.add("inner"))));
          ignored.setRollbackOnly();
        });

    assertEquals(List.of("inner"), calls);
  }

  @Test
  void notSupportedRunsImmediatelyWhileTheOuterTransactionLaterRollsBack() {
    TransactionTemplate outer = new TransactionTemplate(manager);
    TransactionTemplate suspended = new TransactionTemplate(manager);
    suspended.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
    List<String> calls = new ArrayList<>();

    outer.executeWithoutResult(
        ignored -> {
          suspended.executeWithoutResult(
              alsoIgnored -> {
                assertEquals(
                    Result.NO_ACTIVE_TRANSACTION,
                    registrar.registerAfterCommit(() -> calls.add("immediate")));
                calls.add("immediate");
              });
          ignored.setRollbackOnly();
        });

    assertEquals(List.of("immediate"), calls);
  }

  @Test
  void sequentialTransactionsKeepSeparateCallbackQueues() {
    TransactionTemplate transaction = new TransactionTemplate(manager);
    List<String> calls = new ArrayList<>();

    transaction.executeWithoutResult(
        ignored -> registrar.registerAfterCommit(() -> calls.add("first")));
    transaction.executeWithoutResult(
        ignored -> registrar.registerAfterCommit(() -> calls.add("second")));

    assertEquals(List.of("first", "second"), calls);
  }

  @Test
  void afterCompletionCanStartRequiresNewTransactionWithItsOwnRegistrarQueue() {
    TransactionTemplate outer = new TransactionTemplate(manager);
    TransactionTemplate inner = new TransactionTemplate(manager);
    inner.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    List<String> calls = new ArrayList<>();

    outer.executeWithoutResult(
        ignored ->
            registrar.registerAfterCommit(
                () -> {
                  calls.add("outer");
                  inner.executeWithoutResult(
                      alsoIgnored ->
                          assertEquals(
                              Result.REGISTERED,
                              registrar.registerAfterCommit(() -> calls.add("inner"))));
                }));

    assertEquals(List.of("outer", "inner"), calls);
  }

  @Test
  void requiredWorkSubmittedByCommittedCallbackCommitsIndependently() {
    JdbcTemplate jdbc = new JdbcTemplate(manager.getDataSource());
    jdbc.execute("create table callback_work (id integer primary key)");
    TransactionTemplate required = new TransactionTemplate(manager);
    required.executeWithoutResult(
        status -> {
          jdbc.update("insert into callback_work values (1)");
          registrar.registerAfterCommit(
              () ->
                  required.executeWithoutResult(
                      followup -> {
                        jdbc.update("insert into callback_work values (2)");
                        registrar.registerAfterCommit(
                            () ->
                                assertEquals(
                                    2,
                                    jdbc.queryForObject(
                                        "select count(*) from callback_work", Integer.class)));
                      }));
        });
    assertEquals(
        List.of(1, 2),
        jdbc.queryForList("select id from callback_work order by id", Integer.class));
    required.executeWithoutResult(
        status -> {
          registrar.registerAfterCommit(
              () ->
                  required.executeWithoutResult(
                      followup -> jdbc.update("insert into callback_work values (3)")));
          status.setRollbackOnly();
        });
    assertEquals(2, jdbc.queryForObject("select count(*) from callback_work", Integer.class));
  }

  @Test
  void ambiguousManagerFailsBeforeTheOriginatingTransactionCommits() {
    SpringAfterCommitRegistrar invalid =
        new SpringAfterCommitRegistrar(
            () -> {
              throw new IllegalStateException("ambiguous managers");
            });
    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                new TransactionTemplate(manager)
                    .executeWithoutResult(status -> invalid.registerAfterCommit(() -> {})));
    assertTrue(failure.getMessage().contains("default or @Primary"));
  }

  private static DataSourceTransactionManager transactionManager() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:after-commit-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
    return new DataSourceTransactionManager(dataSource);
  }
}
