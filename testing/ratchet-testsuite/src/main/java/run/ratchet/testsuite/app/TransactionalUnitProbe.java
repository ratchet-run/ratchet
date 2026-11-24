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
package run.ratchet.testsuite.app;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.JobContext;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** Application writes join the transaction opened for each checkpoint unit. */
@ApplicationScoped
public class TransactionalUnitProbe {
  public static final class Attempt {
    public final AtomicInteger bodies = new AtomicInteger();
    public final AtomicInteger writes = new AtomicInteger();
    public final List<Optional<String>> received = new CopyOnWriteArrayList<>();
    public final List<Optional<String>> starts = new CopyOnWriteArrayList<>();
    public final CountDownLatch inside = new CountDownLatch(1);
    public final CountDownLatch release = new CountDownLatch(1);
    public final CountDownLatch finished = new CountDownLatch(1);
    public final AtomicReference<Exception> failure = new AtomicReference<>();
    public final AtomicReference<JobContext> context = new AtomicReference<>();
    public volatile int blockedPage = -1;
    public volatile boolean unitRan;
    public volatile String retryFirst;
    public volatile boolean timeoutInterrupted;
  }

  private static final Map<String, Attempt> ATTEMPTS = new ConcurrentHashMap<>();
  @Inject private RatchetEntityManagerProvider entityManagers;

  public static Attempt prepare(String key) {
    Attempt attempt = new Attempt();
    ATTEMPTS.put(key, attempt);
    return attempt;
  }

  public static void reset() {
    ATTEMPTS.values().forEach(attempt -> attempt.release.countDown());
    ATTEMPTS.clear();
  }

  public static String pageKey(String key, int page) {
    return UUID.nameUUIDFromBytes((key + ":" + page).getBytes(UTF_8)).toString();
  }

  public static void execute(String key, String scenario) throws Exception {
    Attempt attempt = ATTEMPTS.get(key);
    var probe = CDI.current().select(TransactionalUnitProbe.class).get();
    JobContext ctx = JobContext.current();
    attempt.context.set(ctx);
    int body = attempt.bodies.incrementAndGet();
    attempt.starts.add(ctx.lastCheckpoint());
    try {
      if (scenario.equals("pressure")) {
        // The test owns the deadline for setup and releases this latch even when setup fails.
        attempt.release.await();
      }
      if (scenario.equals("enclosing")) {
        try {
          probe.runUnitInsideTransaction(key);
        } catch (IllegalStateException failure) {
          attempt.failure.set(failure);
        }
        return;
      }
      if (scenario.equals("progress")) ctx.progress(1, 3);
      int pages =
          switch (scenario) {
            case "atomic", "blocked", "progress" -> 1;
            case "rollback", "timeout" -> 2;
            default -> 3;
          };
      boolean first = true;
      while (true) {
        boolean firstUnit = first;
        String next =
            ctx.runInTransaction(
                cp -> {
                  if (body > 1 && firstUnit) attempt.retryFirst = cp;
                  attempt.unitRan = true;
                  attempt.received.add(Optional.ofNullable(cp));
                  int page = cp == null ? 0 : Integer.parseInt(cp);
                  probe.insert(pageKey(key, page));
                  attempt.writes.incrementAndGet();
                  if ((scenario.equals("rollback") || scenario.equals("timeout"))
                      && page == 1
                      && body == 1) {
                    if (scenario.equals("timeout")) {
                      // Only the hard watchdog interrupt should end this first attempt.
                      try {
                        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                      } catch (InterruptedException interrupted) {
                        attempt.timeoutInterrupted = true;
                        throw interrupted;
                      }
                      throw new IllegalStateException("Hard watchdog did not interrupt the unit");
                    }
                    throw new IOException("Roll back this unit before retry");
                  }
                  if (scenario.equals("atomic")
                      || scenario.equals("blocked")
                      || scenario.equals("progress")) {
                    attempt.blockedPage = page;
                    attempt.inside.countDown();
                    if (!attempt.release.await(
                        scenario.equals("progress") ? 30 : 10, TimeUnit.SECONDS))
                      throw new IllegalStateException("Unit not released");
                  }
                  if (page + 1 == pages && !scenario.equals("atomic")) return "done";
                  return Integer.toString(page + 1);
                });
        first = false;
        if (scenario.equals("retry") && body == 1 && next.equals("2"))
          throw new IOException("Retry after the second committed unit");
        if (next.equals("done") || scenario.equals("atomic")) return;
      }
    } catch (Exception failure) {
      attempt.failure.set(failure);
      throw failure;
    } finally {
      attempt.finished.countDown();
    }
  }

  @Transactional
  public void runUnitInsideTransaction(String key) throws Exception {
    JobContext.current()
        .runInTransaction(
            cp -> {
              ATTEMPTS.get(key).unitRan = true;
              insert(pageKey(key, 0));
              return "1";
            });
  }

  // No new transaction: this write must join the unit's transaction.
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
  public void reserveConnection(
      String sessionQuery, CompletableFuture<Long> ready, CountDownLatch release)
      throws InterruptedException {
    Number session =
        (Number)
            entityManagers.getEntityManager().createNativeQuery(sessionQuery).getSingleResult();
    ready.complete(session.longValue());
    if (!release.await(90, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Datasource reservation was not released");
    }
  }

  @Transactional(TxType.REQUIRES_NEW)
  public long countPage(String key, int page) {
    return count(pageKey(key, page));
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
  public int moveClaim(UUID id, long expectedClaim) {
    return entityManagers
        .getEntityManager()
        .createNativeQuery(
            """
            UPDATE scheduler_job_queue SET claim_seq = claim_seq + 1
            WHERE job_id = ?1 AND claim_seq = ?2 AND status = 'RUNNING'
            """)
        .setParameter(1, SqlDialectTestSupportProvider.get().jobIdParam(id))
        .setParameter(2, expectedClaim)
        .executeUpdate();
  }

  @Transactional(TxType.REQUIRES_NEW)
  public void clear() {
    entityManagers
        .getEntityManager()
        .createNativeQuery("DELETE FROM test_transactional_effects")
        .executeUpdate();
  }
}
