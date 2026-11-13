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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.TransactionalJob;
import run.ratchet.quarkus.it.tck.QuarkusRatchetTckProfile;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** Exercises transactional completion through the SQL extension and its real JTA carrier. */
@QuarkusTest
@QuarkusTestResource(RatchetDatabaseTestResource.class)
@TestProfile(QuarkusRatchetTckProfile.class)
@DisabledIfSystemProperty(named = "quarkus.datasource.db-kind", matches = "mongodb")
class TransactionalCompletionTest {
  @Inject JobSchedulerService scheduler;
  @Inject JobCrudStore jobs;
  @Inject Instance<SqlProbe> probes;
  private String key;

  @BeforeEach
  void prepare() {
    key = UUID.randomUUID().toString();
  }

  @AfterEach
  void cleanUp() {
    probes.get().delete(key);
  }

  @Test
  void successCommitsApplicationRowAndSucceededTogether() {
    String value = key;
    JobHandle handle = scheduler.enqueue(() -> SqlProbe.succeed(value)).withMaxRetries(0).submit();
    awaitStatus(handle, JobStatus.SUCCEEDED);
    assertTrue(probes.get().wrote(key), "Body must finish its insert");
    assertEquals(1, probes.get().count(key));
  }

  @Test
  void bodyFailureRollsBackApplicationWriteAndRecordsFailed() {
    String value = key;
    JobHandle handle = scheduler.enqueue(() -> SqlProbe.fail(value)).withMaxRetries(0).submit();
    awaitStatus(handle, JobStatus.FAILED);
    assertTrue(probes.get().wrote(key), "Failing body must finish its insert before throwing");
    assertEquals(0, probes.get().count(key));
  }

  private void awaitStatus(JobHandle handle, JobStatus expected) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> assertEquals(expected, jobs.findById(handle.id()).orElseThrow().getStatus()));
  }

  @ApplicationScoped
  @UnlessBuildProfile("mongo")
  public static class SqlProbe {
    @Inject RatchetEntityManagerProvider entityManagers;
    private final Set<String> writes = ConcurrentHashMap.newKeySet();

    public boolean wrote(String key) {
      return writes.contains(key);
    }

    @TransactionalJob
    public static void succeed(String key) {
      CDI.current().select(SqlProbe.class).get().insert(key);
    }

    @TransactionalJob
    public static void fail(String key) {
      CDI.current().select(SqlProbe.class).get().insert(key);
      throw new IllegalStateException("Roll back the application's write");
    }

    // The extension's named persistence unit uses the same datasource as Ratchet's SQL store.
    // Joining its existing job transaction is essential; do not start a new transaction here.
    public void insert(String key) {
      entityManagers
          .getEntityManager()
          .createNativeQuery(
              "INSERT INTO test_transactional_effects (entry_id, test_key) VALUES (?1, ?2)")
          .setParameter(1, UUID.randomUUID().toString())
          .setParameter(2, key)
          .executeUpdate();
      writes.add(key);
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
      writes.remove(key);
    }
  }
}
