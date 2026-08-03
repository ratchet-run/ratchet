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
package run.ratchet.store.oracle;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

class OracleJobStoreInitializationTest extends OracleTestFixture {

  private static final List<String> DELEGATE_FIELDS =
      List.of(
          "jobs",
          "query",
          "claims",
          "lifecycle",
          "batches",
          "nodeLocks",
          "archives",
          "auxiliary",
          "tags",
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
    RatchetEntityManagerProvider provider =
        () -> {
          providerCalls.incrementAndGet();
          return fixtureEntityManager;
        };

    OracleJobStore store =
        OracleJobStoreFactory.create(provider, fixtureMetricsCollector, RatchetOptions.defaults());
    Object[] firstDelegates = delegateSnapshot(store);

    ((OracleJobStoreImpl) store).checkIsolationLevel();
    Object[] secondDelegates = delegateSnapshot(store);

    assertEquals(1, providerCalls.get());
    for (int i = 0; i < firstDelegates.length; i++) {
      assertNotNull(firstDelegates[i], DELEGATE_FIELDS.get(i));
      assertSame(firstDelegates[i], secondDelegates[i], DELEGATE_FIELDS.get(i));
    }
  }

  @Test
  void failedInitializationCanBeRetriedAfterProviderIsFixed() {
    AtomicInteger providerCalls = new AtomicInteger();
    AtomicBoolean providerFails = new AtomicBoolean(true);
    OracleJobStoreImpl store =
        new OracleJobStoreImpl(
            () -> {
              providerCalls.incrementAndGet();
              if (providerFails.get()) {
                throw new IllegalStateException("Entity manager unavailable");
              }
              return fixtureEntityManager;
            },
            fixtureMetricsCollector,
            RatchetOptions.defaults());

    assertThrows(IllegalStateException.class, store::checkIsolationLevel);
    assertNull(delegate(store, "jobs"));

    providerFails.set(false);
    store.checkIsolationLevel();

    assertEquals(2, providerCalls.get());
    for (Object initializedDelegate : delegateSnapshot(store)) {
      assertNotNull(initializedDelegate);
    }
  }

  @Test
  void concurrentInitializationBlocksSecondCallerUntilDelegatesAreReady() throws Exception {
    CountDownLatch providerEntered = new CountDownLatch(1);
    CountDownLatch releaseProvider = new CountDownLatch(1);
    CountDownLatch secondCallerStarted = new CountDownLatch(1);
    AtomicInteger providerCalls = new AtomicInteger();
    OracleJobStoreImpl store =
        new OracleJobStoreImpl(
            () -> {
              providerCalls.incrementAndGet();
              providerEntered.countDown();
              try {
                if (!releaseProvider.await(5, SECONDS)) {
                  throw new IllegalStateException("Timed out waiting to release provider");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting to release provider", e);
              }
              return fixtureEntityManager;
            },
            fixtureMetricsCollector,
            RatchetOptions.defaults());
    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      Future<?> first = executor.submit(store::checkIsolationLevel);
      assertTrue(providerEntered.await(5, SECONDS));
      Future<?> second =
          executor.submit(
              () -> {
                secondCallerStarted.countDown();
                store.checkIsolationLevel();
              });

      assertTrue(secondCallerStarted.await(5, SECONDS));
      assertThrows(TimeoutException.class, () -> second.get(1, SECONDS));
      assertEquals(1, providerCalls.get());
      for (Object incompleteDelegate : delegateSnapshot(store)) {
        assertNull(incompleteDelegate);
      }

      releaseProvider.countDown();
      first.get(5, SECONDS);
      second.get(5, SECONDS);
    } finally {
      releaseProvider.countDown();
      executor.shutdownNow();
    }

    assertEquals(1, providerCalls.get());
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
                    OracleJobStoreFactory.create(
                        null, fixtureMetricsCollector, RatchetOptions.defaults())),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> OracleJobStoreFactory.create(provider, null, RatchetOptions.defaults())),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> OracleJobStoreFactory.create(provider, fixtureMetricsCollector, null)));
  }

  private static Object[] delegateSnapshot(Object store) {
    return DELEGATE_FIELDS.stream().map(name -> delegate(store, name)).toArray();
  }

  private static Object delegate(Object store, String fieldName) {
    try {
      Field field = OracleJobStoreImpl.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(store);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Could not inspect " + fieldName, e);
    }
  }
}
