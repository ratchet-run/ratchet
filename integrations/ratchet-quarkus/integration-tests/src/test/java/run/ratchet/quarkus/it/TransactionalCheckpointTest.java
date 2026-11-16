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
package run.ratchet.quarkus.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.quarkus.it.tck.QuarkusRatchetTckProfile;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

@QuarkusTest
@QuarkusTestResource(RatchetDatabaseTestResource.class)
@TestProfile(QuarkusRatchetTckProfile.class)
@DisabledIfSystemProperty(named = "quarkus.datasource.db-kind", matches = "mongodb")
class TransactionalCheckpointTest {
  @Inject JobSchedulerService scheduler;
  @Inject JobCrudStore jobs;
  @Inject Instance<UnitProbe> probes;
  private String key;
  private Observation observation;

  @BeforeEach
  void prepare() {
    key = UUID.randomUUID().toString();
    observation = new Observation();
    UnitProbe.OBSERVED.put(key, observation);
  }

  @AfterEach
  void cleanUp() {
    for (int page = 0; page < 3; page++) probes.get().delete(pageKey(key, page));
    UnitProbe.OBSERVED.remove(key);
  }

  @Test
  void retryResumesAfterLastCommittedUnit() {
    run("retry");
    assertEquals("2", observation.retryFirst);
    assertEquals(
        List.of(Optional.empty(), Optional.of("1"), Optional.of("2")), observation.received);
    for (int page = 0; page < 3; page++) assertEquals(1, probes.get().count(pageKey(key, page)));
  }

  @Test
  void failedUnitRollsBackApplicationWrite() {
    run("failure");
    assertEquals(1, probes.get().count(pageKey(key, 0)));
    assertEquals(0, probes.get().count(pageKey(key, 1)));
    assertEquals(Optional.of("1"), observation.afterFailure);
    assertSame(observation.failure, observation.caught);
  }

  @Test
  void enclosingTransactionIsRejected() {
    run("enclosing");
    assertInstanceOf(IllegalStateException.class, observation.caught);
    assertFalse(observation.unitRan);
    assertEquals(0, probes.get().count(pageKey(key, 0)));
  }

  private void run(String scenario) {
    String value = key;
    var handle =
        scheduler
            .enqueue(() -> UnitProbe.execute(value, scenario))
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
            .submit();
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertEquals(
                    JobStatus.SUCCEEDED, jobs.findById(handle.id()).orElseThrow().getStatus()));
  }

  static String pageKey(String key, int page) {
    return UUID.nameUUIDFromBytes((key + ":" + page).getBytes(UTF_8)).toString();
  }

  static class Observation {
    final AtomicInteger attempts = new AtomicInteger();
    final List<Optional<String>> received = new CopyOnWriteArrayList<>();
    final IOException failure = new IOException("unit failure");
    volatile String retryFirst;
    volatile Optional<String> afterFailure;
    volatile Exception caught;
    volatile boolean unitRan;
  }

  @ApplicationScoped
  @UnlessBuildProfile("mongo")
  public static class UnitProbe {
    static final Map<String, Observation> OBSERVED = new ConcurrentHashMap<>();
    @Inject RatchetEntityManagerProvider entityManagers;

    public static void execute(String key, String scenario) throws Exception {
      var probe = CDI.current().select(UnitProbe.class).get();
      var observation = OBSERVED.get(key);
      var ctx = JobContext.current();
      int attempt = observation.attempts.incrementAndGet();
      if (scenario.equals("enclosing")) {
        try {
          probe.runInsideTransaction(key);
        } catch (IllegalStateException failure) {
          observation.caught = failure;
        }
        return;
      }
      if (scenario.equals("failure")) {
        ctx.runInTransaction(
            cp -> {
              probe.insert(pageKey(key, 0));
              return "1";
            });
        try {
          ctx.runInTransaction(
              cp -> {
                probe.insert(pageKey(key, 1));
                throw observation.failure;
              });
        } catch (IOException failure) {
          observation.caught = failure;
        }
        observation.afterFailure = ctx.lastCheckpoint();
        return;
      }
      boolean first = true;
      while (true) {
        boolean firstUnit = first;
        String next =
            ctx.runInTransaction(
                cp -> {
                  observation.received.add(Optional.ofNullable(cp));
                  if (attempt == 2 && firstUnit) observation.retryFirst = cp;
                  int page = cp == null ? 0 : Integer.parseInt(cp);
                  probe.insert(pageKey(key, page));
                  return page == 2 ? "done" : Integer.toString(page + 1);
                });
        first = false;
        if (attempt == 1 && next.equals("2")) throw new IOException("Retry after committed unit");
        if (next.equals("done")) return;
      }
    }

    @Transactional
    public void runInsideTransaction(String key) throws Exception {
      JobContext.current()
          .runInTransaction(
              cp -> {
                OBSERVED.get(key).unitRan = true;
                insert(pageKey(key, 0));
                return "1";
              });
    }

    // Join the current unit transaction on Ratchet's datasource.
    public void insert(String key) {
      entityManagers
          .getEntityManager()
          .createNativeQuery(
              "INSERT INTO test_transactional_effects (entry_id, test_key) VALUES (?1, ?2)")
          .setParameter(1, UUID.randomUUID().toString())
          .setParameter(2, key)
          .executeUpdate();
    }

    @Transactional(TxType.REQUIRES_NEW)
    public long count(String key) {
      return ((Number)
              entityManagers
                  .getEntityManager()
                  .createNativeQuery(
                      "SELECT COUNT(*) FROM test_transactional_effects WHERE test_key = ?1")
                  .setParameter(1, key)
                  .getSingleResult())
          .longValue();
    }

    @Transactional(TxType.REQUIRES_NEW)
    public void delete(String key) {
      entityManagers
          .getEntityManager()
          .createNativeQuery("DELETE FROM test_transactional_effects WHERE test_key = ?1")
          .setParameter(1, key)
          .executeUpdate();
    }
  }
}
