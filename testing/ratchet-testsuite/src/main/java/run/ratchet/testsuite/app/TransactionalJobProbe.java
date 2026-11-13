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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.JobContext;
import run.ratchet.api.TransactionalJob;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** Application writes through the exact persistence unit used by the SQL store. */
@ApplicationScoped
public class TransactionalJobProbe {
  public static final class Attempt {
    public final AtomicInteger bodies = new AtomicInteger();
    public final AtomicInteger writes = new AtomicInteger();
    public final AtomicReference<JobContext> context = new AtomicReference<>();
    public final CountDownLatch written = new CountDownLatch(1);
    public final CountDownLatch release = new CountDownLatch(1);
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

  @TransactionalJob
  public static void succeed(String key) throws InterruptedException {
    execute(key, false, false);
  }

  @TransactionalJob
  public static void retry(String key) throws InterruptedException {
    execute(key, true, false);
  }

  @TransactionalJob
  public static void block(String key) throws InterruptedException {
    execute(key, false, true);
  }

  private static void execute(String key, boolean failFirst, boolean block)
      throws InterruptedException {
    Attempt attempt = ATTEMPTS.get(key);
    int body = attempt.bodies.incrementAndGet();
    attempt.context.set(JobContext.current());
    CDI.current().select(TransactionalJobProbe.class).get().insert(key);
    attempt.writes.incrementAndGet();
    attempt.written.countDown();
    if (failFirst && body == 1) throw new IllegalStateException("Roll back first attempt");
    if (block && !attempt.release.await(45, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Transactional body was not released");
    }
  }

  // Deliberately no new transaction: this write must join the job's existing transaction.
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
