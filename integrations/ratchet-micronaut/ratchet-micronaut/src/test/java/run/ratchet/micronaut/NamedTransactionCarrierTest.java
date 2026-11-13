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
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionOperationsRegistry;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.micronaut.internal.RatchetTransactionOperations;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerCycleExecutor;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

class NamedTransactionCarrierTest {
  @Test
  void carrierAndRegistrarUseNamedManagerForCommitAndRollback() throws Exception {
    try (ApplicationContext context =
        TestContexts.engine(
            Map.of(
                "ratchet.jpa.datasource", "jobs",
                "datasources.default.url", "jdbc:h2:mem:default-" + System.nanoTime(),
                "datasources.default.driver-class-name", "org.h2.Driver",
                "datasources.default.username", "sa",
                "datasources.jobs.url", "jdbc:h2:mem:jobs-" + System.nanoTime(),
                "datasources.jobs.driver-class-name", "org.h2.Driver",
                "datasources.jobs.username", "sa"))) {
      Poller poller = mock(Poller.class);
      context.registerSingleton(Poller.class, poller);
      context.start();
      TransactionOperations<?> named =
          context.getBean(TransactionOperations.class, Qualifiers.byName("jobs"));
      TransactionOperations<?> defaults =
          context.getBean(TransactionOperations.class, Qualifiers.byName("default"));
      JobTransaction carrier = context.getBean(JobTransaction.class);
      AfterCommitRegistrar registrar = context.getBean(AfterCommitRegistrar.class);
      AtomicInteger callbacks = new AtomicInteger();
      AtomicInteger nestedCallbacks = new AtomicInteger();
      UUID job = UUID.randomUUID();
      String node = context.getBean(NodeIdentityProvider.class).getNodeId();
      JobStore store = context.getBean(JobStore.class);
      when(poller.tick())
          .thenAnswer(
              invocation -> {
                assertFalse(MicronautTransactions.hasActiveTransaction(named));
                assertFalse(MicronautTransactions.hasActiveTransaction(defaults));
                return 1L;
              });
      carrier.run(
          () -> {
            assertTrue(JobTransaction.active());
            assertTrue(MicronautTransactions.hasActiveTransaction(named));
            assertFalse(MicronautTransactions.hasActiveTransaction(defaults));
            var root =
                MicronautTransactions.rootStatus(named.findTransactionStatus().orElseThrow());
            when(store.resetRunningJob(job, node, 0L))
                .thenAnswer(
                    invocation -> {
                      assertSame(
                          root,
                          MicronautTransactions.rootStatus(
                              named.findTransactionStatus().orElseThrow()));
                      assertFalse(MicronautTransactions.hasActiveTransaction(defaults));
                      assertEquals(
                          AfterCommitRegistrar.Result.REGISTERED,
                          registrar.registerAfterCommit(nestedCallbacks::incrementAndGet));
                      return true;
                    });
            assertTrue(context.getBean(JobStateManager.class).resetJobToPending(job, 0L));
            assertEquals(0, nestedCallbacks.get());
            context
                .getBean(BatchCompletionTransaction.class)
                .complete(
                    () -> {
                      assertTrue(MicronautTransactions.hasActiveTransaction(named));
                      assertFalse(MicronautTransactions.hasActiveTransaction(defaults));
                      assertNotSame(
                          root,
                          MicronautTransactions.rootStatus(
                              named.findTransactionStatus().orElseThrow()));
                      return null;
                    });
            assertEquals(1L, context.getBean(PollerCycleExecutor.class).tick());
            assertSame(
                root,
                MicronautTransactions.rootStatus(named.findTransactionStatus().orElseThrow()));

            assertEquals(
                AfterCommitRegistrar.Result.REGISTERED,
                registrar.registerAfterCommit(
                    () -> {
                      assertFalse(MicronautTransactions.hasActiveTransaction(named));
                      assertFalse(MicronautTransactions.hasActiveTransaction(defaults));
                      callbacks.incrementAndGet();
                    }));
            assertEquals(0, callbacks.get());
            return null;
          });
      assertEquals(1, callbacks.get());
      assertEquals(1, nestedCallbacks.get());
      IOException failure = new IOException("body");
      assertSame(
          failure,
          assertThrows(
              IOException.class,
              () ->
                  carrier.run(
                      () -> {
                        assertEquals(
                            AfterCommitRegistrar.Result.REGISTERED,
                            registrar.registerAfterCommit(callbacks::incrementAndGet));
                        throw failure;
                      })));
      assertEquals(1, callbacks.get());
      assertFalse(JobTransaction.active());
      // A transaction on the unrelated default manager cannot satisfy Ratchet's MANDATORY.
      defaults.execute(
          TransactionDefinition.of(Propagation.REQUIRED),
          status -> {
            assertThrows(
                RuntimeException.class,
                () ->
                    context
                        .getBean(PostExecutionHandler.class)
                        .completeSuccessInCurrentTransaction(
                            new JobEntity(), null, null, null, null, 0, 0));
            return null;
          });
    }
  }

  @Test
  void defaultSelectionMatchesMicronautRegistryAndIsCached() {
    try (ApplicationContext context =
        TestContexts.engine(
            Map.of(
                "datasources.default.url", "jdbc:h2:mem:equivalence-" + System.nanoTime(),
                "datasources.default.driver-class-name", "org.h2.Driver",
                "datasources.default.username", "sa"))) {
      context.registerSingleton(Poller.class, mock(Poller.class));
      context.start();
      var selected = context.getBean(RatchetTransactionOperations.class);
      var expected =
          context
              .getBean(TransactionOperationsRegistry.class)
              .provideSynchronous(TransactionOperations.class, null);
      assertSame(expected, selected.operations());
      assertSame(selected.operations(), selected.operations());
    }
  }
}
