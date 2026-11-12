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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.AT_CAPACITY;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.SUPERSEDED;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ResourcePermitStore;

class MongoResourcePermitSnapshotConflictTest {

  private static final MongoTestFixture fixture = new MongoTestFixture();
  private static final Set<String> WRITES =
      Set.of("updateOne", "deleteOne", "insertOne", "findOneAndUpdate", "replaceOne");

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @BeforeEach
  void cleanup() {
    fixture.cleanupStore();
  }

  @Test
  void sameClaimAcquire_retriesAfterPermitWasReplacedAndReleased() {
    assertStaleSnapshotCannotAcquire(true);
  }

  @Test
  void newPermitAcquire_retriesAfterClaimWasSuperseded() {
    assertStaleSnapshotCannotAcquire(false);
  }

  private void assertStaleSnapshotCannotAcquire(boolean existingPermit) {
    ResourcePermitStore permits = fixture.resourcePermitStore();
    permits.configureResource("one", 1, 100, "test");
    JobEntity first = claimNewJob();
    if (existingPermit) {
      assertEquals(
          ACQUIRED, permits.tryAcquirePermit("one", first.getId(), first.getClaimSeq(), "old"));
    }
    AtomicBoolean hookRan = new AtomicBoolean();
    Runnable hook =
        () -> {
          if (!hookRan.compareAndSet(false, true)) {
            return;
          }
          assertEquals(
              1,
              fixture.store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100).reset());
          JobEntity retry = fixture.store().claimNextBatch(1, "new").get(0);
          assertEquals(first.getId(), retry.getId());
          assertTrue(retry.getClaimSeq() > first.getClaimSeq());
          if (existingPermit) {
            assertEquals(
                ACQUIRED,
                permits.tryAcquirePermit("one", retry.getId(), retry.getClaimSeq(), "new"));
            permits.releasePermit("one", retry.getId(), retry.getClaimSeq());
          }
          JobEntity second = claimNewJob();
          assertEquals(
              ACQUIRED,
              permits.tryAcquirePermit("one", second.getId(), second.getClaimSeq(), "other"));
        };
    MongoAuxiliaryOperations wrapped = fixture.auxiliaryOperations(wrapDatabase(hook));

    assertEquals(
        SUPERSEDED, wrapped.tryAcquirePermit("one", first.getId(), first.getClaimSeq(), "old"));
    assertTrue(hookRan.get());
    JobEntity third = claimNewJob();
    assertEquals(
        AT_CAPACITY, permits.tryAcquirePermit("one", third.getId(), third.getClaimSeq(), "third"));
  }

  private JobEntity claimNewJob() {
    fixture.store().create(fixture.newPendingJob());
    return fixture.store().claimNextBatch(1, "node").get(0);
  }

  private MongoDatabase wrapDatabase(Runnable hook) {
    MongoDatabase database = fixture.database();
    return (MongoDatabase)
        Proxy.newProxyInstance(
            MongoDatabase.class.getClassLoader(),
            new Class<?>[] {MongoDatabase.class},
            (proxy, method, args) -> {
              Object result = invoke(database, method, args);
              if (method.getName().equals("getCollection")
                  && (args[0].equals("scheduler_resource_permit")
                      || args[0].equals("scheduler_resource_limit"))) {
                return wrapCollection((MongoCollection<?>) result, hook);
              }
              return result;
            });
  }

  private MongoCollection<?> wrapCollection(MongoCollection<?> collection, Runnable hook) {
    return (MongoCollection<?>)
        Proxy.newProxyInstance(
            MongoCollection.class.getClassLoader(),
            new Class<?>[] {MongoCollection.class},
            (proxy, method, args) -> {
              if (WRITES.contains(method.getName())) {
                hook.run();
              }
              Object result = invoke(collection, method, args);
              if (result instanceof MongoCollection<?> derived) {
                return wrapCollection(derived, hook);
              }
              return result;
            });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
