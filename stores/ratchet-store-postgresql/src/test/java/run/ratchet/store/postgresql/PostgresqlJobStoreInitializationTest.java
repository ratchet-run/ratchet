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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RatchetEntityManagerProvider;
import run.ratchet.store.util.IsolationCheckFailedException;

class PostgresqlJobStoreInitializationTest extends PostgresqlTestFixture {

  private static final String ISOLATION_QUERY = "SHOW transaction_isolation";
  private static final List<String> DELEGATE_FIELDS =
      List.of(
          "ctx",
          "reservations",
          "tags",
          "jobs",
          "query",
          "batches",
          "claims",
          "lifecycle",
          "nodeLocks",
          "archives",
          "auxiliary",
          "signals",
          "recurringJobs",
          "extensions");

  private EntityManager fixtureEntityManager;
  private MetricsCollector fixtureMetricsCollector;

  @Override
  protected JobStore createStore(EntityManager em, MetricsCollector metrics) {
    fixtureEntityManager = em;
    fixtureMetricsCollector = metrics;
    return super.createStore(em, metrics);
  }

  @Test
  void factoryInitializationAndPostConstructInitializeExactlyOnce() {
    AtomicInteger providerCalls = new AtomicInteger();
    AtomicInteger isolationQueries = new AtomicInteger();
    EntityManager countingEntityManager =
        interceptIsolationQuery(fixtureEntityManager, isolationQueries::incrementAndGet);
    RatchetEntityManagerProvider provider =
        () -> {
          providerCalls.incrementAndGet();
          return countingEntityManager;
        };

    PostgresqlJobStore store =
        PostgresqlJobStoreFactory.create(
            provider, fixtureMetricsCollector, RatchetOptions.defaults());
    Object[] firstDelegates = delegateSnapshot(store);

    ((PostgresqlJobStoreImpl) store).checkIsolationLevel();
    Object[] secondDelegates = delegateSnapshot(store);

    assertEquals(1, providerCalls.get());
    assertEquals(1, isolationQueries.get());
    for (int i = 0; i < firstDelegates.length; i++) {
      assertNotNull(firstDelegates[i], DELEGATE_FIELDS.get(i));
      assertSame(firstDelegates[i], secondDelegates[i], DELEGATE_FIELDS.get(i));
    }
  }

  @Test
  void failedInitializationCanBeRetriedAfterIsolationIsFixed() {
    AtomicInteger providerCalls = new AtomicInteger();
    AtomicInteger isolationQueries = new AtomicInteger();
    AtomicReference<String> isolation = new AtomicReference<>("repeatable read");
    Query query = mock(Query.class);
    when(query.getSingleResult()).thenAnswer(ignored -> isolation.get());
    EntityManager entityManager =
        interceptIsolationQuery(
            fixtureEntityManager,
            () -> {
              isolationQueries.incrementAndGet();
            },
            query);
    PostgresqlJobStoreImpl store =
        new PostgresqlJobStoreImpl(
            () -> {
              providerCalls.incrementAndGet();
              return entityManager;
            },
            fixtureMetricsCollector,
            RatchetOptions.defaults());

    assertThrows(IsolationCheckFailedException.class, store::checkIsolationLevel);
    assertNull(delegate(store, "jobs"));

    isolation.set("read committed");
    store.checkIsolationLevel();

    assertEquals(1, providerCalls.get());
    assertEquals(2, isolationQueries.get());
    for (Object initializedDelegate : delegateSnapshot(store)) {
      assertNotNull(initializedDelegate);
    }
  }

  @Test
  void concurrentInitializationBlocksSecondCallerUntilDelegatesAreReady() throws Exception {
    CountDownLatch isolationQueryEntered = new CountDownLatch(1);
    CountDownLatch releaseIsolationQuery = new CountDownLatch(1);
    CountDownLatch secondCallerStarted = new CountDownLatch(1);
    AtomicInteger isolationQueries = new AtomicInteger();
    EntityManager blockingEntityManager =
        interceptIsolationQuery(
            fixtureEntityManager,
            () -> {
              isolationQueries.incrementAndGet();
              isolationQueryEntered.countDown();
              if (!releaseIsolationQuery.await(5, SECONDS)) {
                throw new IllegalStateException("Timed out waiting to release isolation query");
              }
            });
    PostgresqlJobStoreImpl store =
        new PostgresqlJobStoreImpl(
            () -> blockingEntityManager, fixtureMetricsCollector, RatchetOptions.defaults());
    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      Future<?> first = executor.submit(store::checkIsolationLevel);
      assertTrue(isolationQueryEntered.await(5, SECONDS));
      Future<?> second =
          executor.submit(
              () -> {
                secondCallerStarted.countDown();
                store.checkIsolationLevel();
              });

      assertTrue(secondCallerStarted.await(5, SECONDS));
      assertThrows(TimeoutException.class, () -> second.get(1, SECONDS));
      assertEquals(1, isolationQueries.get());
      for (Object incompleteDelegate : delegateSnapshot(store)) {
        assertNull(incompleteDelegate);
      }

      releaseIsolationQuery.countDown();
      first.get(5, SECONDS);
      second.get(5, SECONDS);
    } finally {
      releaseIsolationQuery.countDown();
      executor.shutdownNow();
    }

    assertEquals(1, isolationQueries.get());
    for (Object initializedDelegate : delegateSnapshot(store)) {
      assertNotNull(initializedDelegate);
    }
  }

  @Test
  void factoryRejectsNullCollaboratorsBeforeInitialization() {
    RatchetEntityManagerProvider provider =
        () -> {
          throw new AssertionError("Factory must validate every argument before initialization");
        };

    assertAll(
        () ->
            assertThrows(
                NullPointerException.class,
                () ->
                    PostgresqlJobStoreFactory.create(
                        null, fixtureMetricsCollector, RatchetOptions.defaults())),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> PostgresqlJobStoreFactory.create(provider, null, RatchetOptions.defaults())),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> PostgresqlJobStoreFactory.create(provider, fixtureMetricsCollector, null)));
  }

  private static EntityManager interceptIsolationQuery(
      EntityManager delegate, IsolationQueryInterceptor interceptor) {
    return interceptIsolationQuery(delegate, interceptor, null);
  }

  private static EntityManager interceptIsolationQuery(
      EntityManager delegate, IsolationQueryInterceptor interceptor, Query replacementQuery) {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())
                  && args != null
                  && args.length > 0
                  && ISOLATION_QUERY.equals(args[0])) {
                interceptor.intercept();
                if (replacementQuery != null) {
                  return replacementQuery;
                }
              }
              try {
                return method.invoke(delegate, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  private static Object[] delegateSnapshot(Object store) {
    return DELEGATE_FIELDS.stream().map(name -> delegate(store, name)).toArray();
  }

  private static Object delegate(Object store, String fieldName) {
    try {
      Field field = PostgresqlJobStoreImpl.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(store);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Could not inspect " + fieldName, e);
    }
  }

  @FunctionalInterface
  private interface IsolationQueryInterceptor {
    void intercept() throws Exception;
  }
}
