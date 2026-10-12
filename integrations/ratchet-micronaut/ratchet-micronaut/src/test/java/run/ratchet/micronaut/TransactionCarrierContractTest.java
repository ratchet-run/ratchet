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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.annotation.Creator;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import jakarta.transaction.Transactional;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.micronaut.internal.tx.MicronautBatchService;
import run.ratchet.micronaut.internal.tx.MicronautChainScheduler;
import run.ratchet.micronaut.internal.tx.MicronautDeadLetterService;
import run.ratchet.micronaut.internal.tx.MicronautDefaultJobArchivingService;
import run.ratchet.micronaut.internal.tx.MicronautDefaultJobCreationService;
import run.ratchet.micronaut.internal.tx.MicronautDefaultJobSchedulerService;
import run.ratchet.micronaut.internal.tx.MicronautDefaultRecurringAnnotationMaintenanceService;
import run.ratchet.micronaut.internal.tx.MicronautJobCascadeService;
import run.ratchet.micronaut.internal.tx.MicronautJobStateManager;
import run.ratchet.micronaut.internal.tx.MicronautPollerCycleExecutor;
import run.ratchet.micronaut.internal.tx.MicronautPostExecutionHandler;
import run.ratchet.micronaut.internal.tx.MicronautRecurringJobExecutor;
import run.ratchet.micronaut.internal.tx.MicronautWorkflowScheduler;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.JobCascadeService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.RecurringJobExecutor;
import run.ratchet.ri.core.internal.ChainScheduler;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultJobArchivingService;
import run.ratchet.ri.core.internal.DefaultRecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerCycleExecutor;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobStore;

class TransactionCarrierContractTest {
  private static final Map<Class<?>, Class<?>> CARRIERS =
      Map.ofEntries(
          Map.entry(MicronautBatchService.class, BatchService.class),
          Map.entry(MicronautChainScheduler.class, ChainScheduler.class),
          Map.entry(MicronautDeadLetterService.class, DeadLetterService.class),
          Map.entry(MicronautDefaultJobArchivingService.class, DefaultJobArchivingService.class),
          Map.entry(MicronautDefaultJobCreationService.class, DefaultJobCreationService.class),
          Map.entry(MicronautDefaultJobSchedulerService.class, DefaultJobSchedulerService.class),
          Map.entry(
              MicronautDefaultRecurringAnnotationMaintenanceService.class,
              DefaultRecurringAnnotationMaintenanceService.class),
          Map.entry(MicronautJobCascadeService.class, JobCascadeService.class),
          Map.entry(MicronautJobStateManager.class, JobStateManager.class),
          Map.entry(MicronautPollerCycleExecutor.class, PollerCycleExecutor.class),
          Map.entry(MicronautPostExecutionHandler.class, PostExecutionHandler.class),
          Map.entry(MicronautRecurringJobExecutor.class, RecurringJobExecutor.class),
          Map.entry(MicronautWorkflowScheduler.class, WorkflowScheduler.class));

  // ClassGraph is not on this module's test classpath. Update this authoritative list whenever
  // core gains or loses a class with Jakarta @Transactional on the class or any method.
  private static final Set<Class<?>> TRANSACTIONAL_CORE =
      Set.of(
          BatchCompletionTransaction.class,
          BatchService.class,
          ChainScheduler.class,
          DeadLetterService.class,
          DefaultJobArchivingService.class,
          DefaultJobCreationService.class,
          DefaultJobSchedulerService.class,
          DefaultRecurringAnnotationMaintenanceService.class,
          JobCascadeService.class,
          JobStateManager.class,
          PollerCycleExecutor.class,
          PostExecutionHandler.class,
          RecurringJobExecutor.class,
          WorkflowScheduler.class);

  @Test
  void carriersPreserveConstructorAndTransactionContracts() throws Exception {
    for (var entry : CARRIERS.entrySet()) {
      Class<?> carrier = entry.getKey();
      Class<?> core = entry.getValue();
      assertEquals(core, carrier.getSuperclass(), carrier.getName());
      var constructor = carrier.getConstructor();
      assertTrue(Modifier.isPublic(constructor.getModifiers()), carrier.getName());
      assertTrue(constructor.isAnnotationPresent(Creator.class), carrier.getName());
      Transactional expected = core.getDeclaredAnnotation(Transactional.class);
      Transactional actual = carrier.getDeclaredAnnotation(Transactional.class);
      if (expected == null) {
        assertNull(actual, carrier.getName());
      } else {
        assertNotNull(actual, carrier.getName());
        assertEquals(expected.value(), actual.value(), carrier.getName());
        assertArrayEquals(expected.rollbackOn(), actual.rollbackOn(), carrier.getName());
        assertArrayEquals(expected.dontRollbackOn(), actual.dontRollbackOn(), carrier.getName());
      }
    }
  }

  @Test
  void proxiedMethodSignaturesAreAccessibleToGeneratedFactoryCode() {
    Set<Class<?>> products = new HashSet<>(CARRIERS.keySet());
    products.add(BatchCompletionTransaction.class);
    for (Class<?> product : products) {
      // FactoryBeanElementCreator advises public, non-final, non-static methods, including
      // inherited methods. The generated Exec lives in the factory's package, not core's.
      for (var method : product.getMethods()) {
        if (Modifier.isFinal(method.getModifiers()) || Modifier.isStatic(method.getModifiers()))
          continue;
        Set<Type> seen = new HashSet<>();
        assertAccessible(method.getGenericReturnType(), seen, method.toString());
        for (Type type : method.getGenericParameterTypes())
          assertAccessible(type, seen, method.toString());
        for (Type type : method.getGenericExceptionTypes())
          assertAccessible(type, seen, method.toString());
      }
    }
  }

  @Test
  void pollerNotSupportedCallsRequiredCarrierWithRealTransaction() {
    try (ApplicationContext context =
        TestContexts.engine(
            Map.of(
                "datasources.default.url",
                    "jdbc:h2:mem:carrier-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
                "datasources.default.driver-class-name", "org.h2.Driver",
                "datasources.default.username", "sa",
                "datasources.default.password", ""))) {
      Poller poller = mock(Poller.class);
      context.registerSingleton(Poller.class, poller);
      context.start();
      TransactionOperations<?> operations = context.getBean(TransactionOperations.class);
      JobStateManager inner = context.getBean(JobStateManager.class);
      JobStore store = context.getBean(JobStore.class);
      String node = context.getBean(NodeIdentityProvider.class).getNodeId();
      UUID job = UUID.randomUUID();
      when(store.resetRunningJob(job, node, 0L))
          .thenAnswer(
              invocation -> {
                assertTrue(MicronautTransactions.hasActiveTransaction(operations));
                Connection connection =
                    (Connection) operations.findTransactionStatus().orElseThrow().getConnection();
                assertFalse(connection.getAutoCommit());
                try (var statement = connection.createStatement()) {
                  statement.execute("insert into carrier_work values (1)");
                }
                return true;
              });
      when(poller.tick())
          .thenAnswer(
              invocation -> {
                assertFalse(MicronautTransactions.hasActiveTransaction(operations));
                assertTrue(inner.resetJobToPending(job, 0L));
                return 1L;
              });
      operations.execute(
          TransactionDefinition.of(Propagation.REQUIRED),
          status -> {
            try (var statement = ((Connection) status.getConnection()).createStatement()) {
              statement.execute("create table carrier_work (id integer)");
            } catch (SQLException failure) {
              throw new IllegalStateException(failure);
            }
            assertEquals(1L, context.getBean(PollerCycleExecutor.class).tick());
            status.setRollbackOnly();
            return null;
          });
      operations.execute(
          TransactionDefinition.of(Propagation.REQUIRED),
          status -> {
            try (var statement = ((Connection) status.getConnection()).createStatement();
                var rows = statement.executeQuery("select count(*) from carrier_work")) {
              assertTrue(rows.next());
              assertEquals(1, rows.getInt(1));
            } catch (SQLException failure) {
              throw new IllegalStateException(failure);
            }
            return null;
          });
      verify(store).resetRunningJob(job, node, 0L);
    }
  }

  private static void assertAccessible(Type type, Set<Type> seen, String method) {
    if (!seen.add(type)) return;
    if (type instanceof Class<?> clazz) {
      if (clazz.isArray()) {
        assertAccessible(clazz.getComponentType(), seen, method);
      } else if (!clazz.isPrimitive()) {
        assertTrue(Modifier.isPublic(clazz.getModifiers()), method + " references " + clazz);
        if (clazz.getEnclosingClass() != null)
          assertAccessible(clazz.getEnclosingClass(), seen, method);
      }
    } else if (type instanceof ParameterizedType parameterized) {
      assertAccessible(parameterized.getRawType(), seen, method);
      for (Type argument : parameterized.getActualTypeArguments())
        assertAccessible(argument, seen, method);
    } else if (type instanceof GenericArrayType array) {
      assertAccessible(array.getGenericComponentType(), seen, method);
    } else if (type instanceof TypeVariable<?> variable) {
      for (Type bound : variable.getBounds()) assertAccessible(bound, seen, method);
    } else if (type instanceof WildcardType wildcard) {
      for (Type bound : wildcard.getUpperBounds()) assertAccessible(bound, seen, method);
      for (Type bound : wildcard.getLowerBounds()) assertAccessible(bound, seen, method);
    }
  }

  @Test
  void allKnownTransactionalCoreTypesAreAccountedFor() throws Exception {
    Set<Class<?>> covered = CARRIERS.values().stream().collect(Collectors.toSet());
    covered.add(BatchCompletionTransaction.class);
    assertEquals(TRANSACTIONAL_CORE, covered);
    for (Class<?> core : TRANSACTIONAL_CORE) {
      assertTrue(
          core.isAnnotationPresent(Transactional.class)
              || Arrays.stream(core.getDeclaredMethods())
                  .anyMatch(method -> method.isAnnotationPresent(Transactional.class)),
          core.getName());
    }
    // This plain marker product already satisfies the processor's constructor rule.
    assertTrue(Modifier.isPublic(BatchCompletionTransaction.class.getConstructor().getModifiers()));
  }

  @Test
  void factoriesDeclareCarrierTypesAndCopyClassDefaults() {
    for (Class<?> factory :
        new Class<?>[] {
          RatchetExecutionConfiguration.class,
          RatchetRuntimeConfiguration.class,
          RatchetSubmissionConfiguration.class
        }) {
      for (var method : factory.getDeclaredMethods()) {
        Class<?> type = method.getReturnType();
        assertFalse(CARRIERS.containsValue(type), method.toString());
        if (CARRIERS.containsKey(type) || type == BatchCompletionTransaction.class) {
          assertTrue(
              method.isAnnotationPresent(RatchetTransactionBoundary.class), method.toString());
          Transactional expected = type.getAnnotation(Transactional.class);
          Transactional actual = method.getDeclaredAnnotation(Transactional.class);
          if (expected == null) {
            assertNull(actual, method.toString());
          } else {
            assertNotNull(actual, method.toString());
            assertEquals(expected.value(), actual.value(), method.toString());
            assertArrayEquals(expected.rollbackOn(), actual.rollbackOn(), method.toString());
            assertArrayEquals(
                expected.dontRollbackOn(), actual.dontRollbackOn(), method.toString());
          }
        }
      }
    }
  }
}
