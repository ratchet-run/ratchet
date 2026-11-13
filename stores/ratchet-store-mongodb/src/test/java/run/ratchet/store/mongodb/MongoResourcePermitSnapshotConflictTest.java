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
import com.mongodb.client.model.Filters;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
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
  void generationWriteBeforeReads_observesSupersededClaim() {
    ResourcePermitStore permits = fixture.resourcePermitStore();
    permits.configureResource("one", 1, 100, "test");
    JobEntity first = claimNewJob("one");
    assertEquals(
        ACQUIRED, permits.tryAcquirePermit("one", first.getId(), first.getClaimSeq(), "old"));
    AtomicBoolean hookRan = new AtomicBoolean();
    AtomicReference<JobEntity> retry = new AtomicReference<>();
    Runnable hook =
        () -> {
          if (!hookRan.compareAndSet(false, true)) return;
          assertEquals(
              1,
              fixture.store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100).reset());
          JobEntity reclaimed = fixture.store().claimNextBatch(1, "new").get(0);
          assertEquals(first.getId(), reclaimed.getId());
          assertTrue(reclaimed.getClaimSeq() > first.getClaimSeq());
          retry.set(reclaimed);
        };
    MongoAuxiliaryOperations wrapped = fixture.auxiliaryOperations(wrapDatabase(hook));
    assertEquals(
        SUPERSEDED, wrapped.tryAcquirePermit("one", first.getId(), first.getClaimSeq(), "old"));
    assertTrue(hookRan.get());
    JobEntity reclaimed = retry.get();
    assertEquals(
        AT_CAPACITY,
        permits.tryAcquirePermit("one", reclaimed.getId(), reclaimed.getClaimSeq(), "new"));
    permits.releasePermit("one", first.getId(), first.getClaimSeq());
    assertEquals(
        List.of(first.getId()), permits.wakeWaiters("one").stream().map(w -> w.jobId()).toList());
    reclaimed = fixture.store().claimNextBatch(1, "retry").get(0);
    assertEquals(
        ACQUIRED,
        permits.tryAcquirePermit("one", reclaimed.getId(), reclaimed.getClaimSeq(), "retry"));
  }

  @Test
  void newPermitAcquire_generationWriteObservesSupersededClaim() {
    ResourcePermitStore permits = fixture.resourcePermitStore();
    permits.configureResource("one", 1, 100, "test");
    JobEntity first = claimNewJob("one");
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
          JobEntity second = claimNewJob("one");
          assertEquals(
              ACQUIRED,
              permits.tryAcquirePermit("one", second.getId(), second.getClaimSeq(), "other"));
        };
    MongoAuxiliaryOperations wrapped = fixture.auxiliaryOperations(wrapDatabase(hook));

    assertEquals(
        SUPERSEDED, wrapped.tryAcquirePermit("one", first.getId(), first.getClaimSeq(), "old"));
    assertTrue(hookRan.get());
    JobEntity third = claimNewJob("one");
    assertEquals(
        AT_CAPACITY, permits.tryAcquirePermit("one", third.getId(), third.getClaimSeq(), "third"));
  }

  @Test
  void twoFullPoolAcquiresAndConcurrentWake_doNotLoseWakeup() throws Exception {
    ResourcePermitStore permits = fixture.resourcePermitStore();
    permits.configureResource("one", 1, 30_000, "test");
    JobEntity holder = claimNewJob("one");
    assertEquals(
        ACQUIRED, permits.tryAcquirePermit("one", holder.getId(), holder.getClaimSeq(), "holder"));
    JobEntity first = claimNewJob("one");
    JobEntity second = claimNewJob("one");
    CountDownLatch parked = new CountDownLatch(1);
    CountDownLatch allowCommit = new CountDownLatch(1);
    AtomicBoolean once = new AtomicBoolean();
    CountDownLatch competingWrites = new CountDownLatch(2);
    MongoDatabase observed = observeFirstPark(parked, allowCommit, competingWrites, once);
    MongoAuxiliaryOperations wrapped = fixture.auxiliaryOperations(observed);
    var executor = Executors.newFixedThreadPool(3);
    try {
      var acquireFirst =
          executor.submit(
              () -> wrapped.tryAcquirePermit("one", first.getId(), first.getClaimSeq(), "first"));
      assertTrue(parked.await(10, TimeUnit.SECONDS), "first miss must park before committing");
      CountDownLatch started = new CountDownLatch(2);
      var acquireSecond =
          executor.submit(
              () -> {
                started.countDown();
                return wrapped.tryAcquirePermit(
                    "one", second.getId(), second.getClaimSeq(), "second");
              });
      var wake =
          executor.submit(
              () -> {
                started.countDown();
                wrapped.releasePermit("one", holder.getId(), holder.getClaimSeq());
                return wrapped.wakeWaiters("one");
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      assertTrue(
          competingWrites.await(10, TimeUnit.SECONDS),
          "contenders must attempt resource writes while the first park is uncommitted");
      allowCommit.countDown();
      assertEquals(AT_CAPACITY, acquireFirst.get(20, TimeUnit.SECONDS));
      var secondResult = acquireSecond.get(20, TimeUnit.SECONDS);
      var woken = wake.get(20, TimeUnit.SECONDS);
      assertTrue(secondResult == AT_CAPACITY || secondResult == ACQUIRED);
      if (secondResult == ACQUIRED) {
        permits.releasePermit("one", second.getId(), second.getClaimSeq());
        woken = permits.wakeWaiters("one");
      }
      assertEquals(List.of(first.getId()), woken.stream().map(w -> w.jobId()).toList());
      Document due =
          fixture
              .database()
              .getCollection("scheduler_job")
              .find(Filters.eq("_id", first.getId()))
              .first();
      assertTrue(!due.getDate("scheduled_time").after(Date.from(Instant.now())));
      Document limit =
          fixture
              .database()
              .getCollection("scheduler_resource_limit")
              .find(Filters.eq("_id", "one"))
              .first();
      assertTrue(((Number) limit.get("generation")).longValue() >= 4);
    } finally {
      allowCommit.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private MongoDatabase observeFirstPark(
      CountDownLatch parked,
      CountDownLatch allowCommit,
      CountDownLatch competingWrites,
      AtomicBoolean once) {
    AtomicBoolean acquireAttempt = new AtomicBoolean();
    AtomicBoolean releaseAttempt = new AtomicBoolean();
    MongoDatabase database = fixture.database();
    return (MongoDatabase)
        Proxy.newProxyInstance(
            MongoDatabase.class.getClassLoader(),
            new Class<?>[] {MongoDatabase.class},
            (proxy, method, args) -> {
              Object result = invoke(database, method, args);
              if (method.getName().equals("getCollection")
                  && (args[0].equals("scheduler_resource_waiter")
                      || args[0].equals("scheduler_resource_limit"))) {
                MongoCollection<?> collection = (MongoCollection<?>) result;
                boolean waiterCollection = args[0].equals("scheduler_resource_waiter");
                return Proxy.newProxyInstance(
                    MongoCollection.class.getClassLoader(),
                    new Class<?>[] {MongoCollection.class},
                    (cp, cm, ca) -> {
                      if (!waiterCollection && parked.getCount() == 0) {
                        if (cm.getName().equals("findOneAndUpdate")
                            && acquireAttempt.compareAndSet(false, true)) {
                          competingWrites.countDown();
                        }
                        if (cm.getName().equals("updateOne")
                            && releaseAttempt.compareAndSet(false, true)) {
                          competingWrites.countDown();
                        }
                      }
                      Object value = invoke(collection, cm, ca);
                      if (waiterCollection
                          && cm.getName().equals("updateOne")
                          && once.compareAndSet(false, true)) {
                        parked.countDown();
                        assertTrue(allowCommit.await(10, TimeUnit.SECONDS));
                      }
                      return value;
                    });
              }
              return result;
            });
  }

  private JobEntity claimNewJob(String resource) {
    var job = fixture.newPendingJob();
    job.setResourceName(resource);
    fixture.store().create(job);
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
