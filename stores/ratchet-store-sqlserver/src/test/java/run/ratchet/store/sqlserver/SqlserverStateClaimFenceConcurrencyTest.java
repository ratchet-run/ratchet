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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Fenced writes use real transactions without the shared fixture's transient retry wrapper. */
class SqlserverStateClaimFenceConcurrencyTest {
  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @Test
  void writesRaceReclaim() {
    exercise("claim");
  }

  @Test
  void writesRaceCancel() {
    exercise("cancel");
  }

  @Test
  void writesRaceTerminalFailure() {
    exercise("failure");
  }

  @Test
  void writesRaceOrphanRecovery() {
    exercise("recovery");
  }

  private void exercise(String transition) {
    var container = fixture.container();
    var properties = new HashMap<String, Object>(fixture.jpaProperties());
    properties.put("jakarta.persistence.jdbc.url", container.getJdbcUrl());
    properties.put("jakarta.persistence.jdbc.user", container.getUsername());
    properties.put("jakarta.persistence.jdbc.password", container.getPassword());
    properties.put("hibernate.connection.pool_size", "4");
    EntityManagerFactory emf =
        Persistence.createEntityManagerFactory("ratchet-sqlserver-tck", properties);
    try {
      fixture.cleanupStore();
      for (int round = 0; round < 4; round++) {
        UUID id = tx(emf, store -> store.create(fixture.newPendingJob()).getId());
        long claim = tx(emf, store -> store.claimNextBatch(1, "state-owner").get(0).getClaimSeq());
        assertEquals(
            StateWriteResult.UPDATED,
            tx(emf, store -> store.initState(id, "race", "initial", claim)));
        AtomicInteger updated = new AtomicInteger();
        AtomicBoolean transitioned = new AtomicBoolean();
        var initialized = new HashMap<String, StateWriteResult>();
        var failures =
            ConcurrentTestRunner.runAll(
                Duration.ofSeconds(15),
                () -> {
                  for (int attempt = 0; attempt < 24; attempt++) {
                    boolean afterTransition = transitioned.get();
                    int version = updated.get();
                    StateWriteResult result =
                        tx(emf, store -> store.updateState(id, "race", "write", version, claim));
                    if (afterTransition) assertEquals(StateWriteResult.SUPERSEDED, result);
                    if (result == StateWriteResult.UPDATED) updated.incrementAndGet();
                    String namespace = "init-" + attempt;
                    afterTransition = transitioned.get();
                    StateWriteResult initResult =
                        tx(emf, store -> store.initState(id, namespace, "initial", claim));
                    if (afterTransition) assertEquals(StateWriteResult.SUPERSEDED, initResult);
                    initialized.put(namespace, initResult);
                  }
                },
                () -> {
                  switch (transition) {
                    case "cancel" -> tx(emf, store -> store.cancelJob(id));
                    case "failure" ->
                        tx(emf, store -> store.markJobFailedTerminal(id, "timeout", 1, claim));
                    default -> {
                      tx(
                          emf,
                          store ->
                              store.resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100));
                      if (transition.equals("claim")) {
                        reclaim(emf, id, claim);
                      }
                    }
                  }
                  transitioned.set(true);
                });
        // Fail on every surfaced exception, including SQL Server 1205 and MySQL 1213.
        assertTrue(failures.stream().allMatch(failure -> failure == null), failures.toString());
        assertTrue(transitioned.get());
        initialized.forEach(
            (namespace, result) -> {
              var state = tx(emf, store -> store.getState(id, namespace));
              if (result == StateWriteResult.UPDATED) {
                assertEquals(0, state.orElseThrow().version(), namespace);
                assertEquals("initial", state.orElseThrow().json(), namespace);
              } else {
                assertEquals(StateWriteResult.SUPERSEDED, result, namespace);
                assertTrue(state.isEmpty(), namespace);
              }
            });
        assertEquals(
            StateWriteResult.SUPERSEDED,
            tx(emf, store -> store.initState(id, "after-transition", "stale", claim)));
        assertTrue(tx(emf, store -> store.getState(id, "after-transition")).isEmpty());
        assertEquals(
            updated.get(),
            tx(emf, store -> store.getState(id, "race").orElseThrow().version()).intValue());
        assertEquals(
            StateWriteResult.SUPERSEDED,
            tx(emf, store -> store.updateState(id, "race", "stale", updated.get(), claim)));
        assertEquals(
            updated.get(),
            tx(emf, store -> store.getState(id, "race").orElseThrow().version()).intValue());
        if (transition.equals("claim")) {
          long nextClaim = tx(emf, store -> store.findById(id).orElseThrow().getClaimSeq());
          assertTrue(nextClaim > claim);
          assertEquals(
              StateWriteResult.UPDATED,
              tx(
                  emf,
                  store -> store.updateState(id, "race", "new owner", updated.get(), nextClaim)));
          assertEquals(
              updated.get() + 1,
              tx(emf, store -> store.getState(id, "race").orElseThrow().version()).intValue());
        }
        fixture.cleanupStore();
      }
    } finally {
      try {
        fixture.cleanupStore();
      } finally {
        emf.close();
      }
    }
  }

  private void reclaim(EntityManagerFactory emf, UUID id, long oldClaim) {
    for (int attempt = 0; attempt < 100; attempt++) {
      var claimed = tx(emf, store -> store.claimNextBatch(1, "next-owner"));
      if (!claimed.isEmpty()) {
        assertEquals(id, claimed.get(0).getId());
        assertTrue(claimed.get(0).getClaimSeq() > oldClaim);
        return;
      }
      // SKIP LOCKED may skip the queue row while a fenced write holds a shared lock.
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted while waiting to reclaim job " + id, e);
      }
    }
    fail("Failed to reclaim job " + id + " after 100 claim attempts");
  }

  private <T> T tx(EntityManagerFactory emf, Function<SqlserverJobStoreImpl, T> work) {
    var em = emf.createEntityManager();
    var transaction = em.getTransaction();
    try {
      transaction.begin();
      var store =
          new SqlserverJobStoreImpl(
              () -> em, mock(MetricsCollector.class), RatchetOptions.defaults());
      store.checkIsolationLevel();
      T result = work.apply(store);
      transaction.commit();
      return result;
    } finally {
      if (transaction.isActive()) transaction.rollback();
      em.close();
    }
  }
}
