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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code ResourcePermitStore}. */
public abstract class AbstractResourcePermitStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupResourcePermitFixture() {
    cleanupStore();
  }

  @Test
  void configureAndAcquirePermit_succeeds() {
    resourcePermitStore().configureResource("res-1", 2, 1000, "test");
    var job = persist(newPendingJob());

    boolean acquired = resourcePermitStore().tryAcquirePermit("res-1", job.getId(), "node-1");

    assertTrue(acquired, "tryAcquirePermit should succeed when capacity is available");
  }

  @Test
  void tryAcquirePermit_failsAtCapacity() {
    resourcePermitStore().configureResource("res-limited", 1, 1000, "limited resource");
    var firstJob = persist(newPendingJob());
    var secondJob = persist(newPendingJob());

    boolean firstAcquired =
        resourcePermitStore().tryAcquirePermit("res-limited", firstJob.getId(), "node-1");
    boolean secondAcquired =
        resourcePermitStore().tryAcquirePermit("res-limited", secondJob.getId(), "node-1");

    assertTrue(firstAcquired, "First acquire should succeed");
    assertFalse(secondAcquired, "Second acquire should fail when resource is at capacity");
  }

  @Test
  void tryAcquirePermit_unconfiguredResource_failsHard() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> resourcePermitStore().tryAcquirePermit("missing-resource", job.getId(), "node-1"),
        "Unconfigured resources should fail hard instead of behaving like capacity pressure");
  }

  @Test
  void releasePermit_freesCapacity() {
    resourcePermitStore().configureResource("res-release", 1, 1000, "release test");
    var firstJob = persist(newPendingJob());
    var secondJob = persist(newPendingJob());

    resourcePermitStore().tryAcquirePermit("res-release", firstJob.getId(), "node-1");
    resourcePermitStore().releasePermit("res-release", firstJob.getId());

    boolean acquired =
        resourcePermitStore().tryAcquirePermit("res-release", secondJob.getId(), "node-1");

    assertTrue(acquired, "Acquire should succeed after a permit has been released");
  }

  @Test
  void releaseAllPermits_freesAllForJob() {
    resourcePermitStore().configureResource("res-A", 2, 1000, "resource A");
    resourcePermitStore().configureResource("res-B", 2, 1000, "resource B");
    var job = persist(newPendingJob());

    resourcePermitStore().tryAcquirePermit("res-A", job.getId(), "node-1");
    resourcePermitStore().tryAcquirePermit("res-B", job.getId(), "node-1");

    resourcePermitStore().releaseAllPermits(job.getId());

    var otherJob = persist(newPendingJob());
    assertTrue(
        resourcePermitStore().tryAcquirePermit("res-A", otherJob.getId(), "node-1"),
        "Resource A should be freed after releaseAllPermits");
    assertTrue(
        resourcePermitStore().tryAcquirePermit("res-B", otherJob.getId(), "node-1"),
        "Resource B should be freed after releaseAllPermits");
  }

  @Test
  void getPermitRetryDelay_returnsConfiguredDelay() {
    resourcePermitStore().configureResource("res-delay", 1, 2500, "delay test");

    int delay = resourcePermitStore().getPermitRetryDelay("res-delay");

    assertEquals(2500, delay, "getPermitRetryDelay should return the configured delay");
  }

  @Test
  void getPermitRetryDelay_unknownResource_returnsDefaultDelay() {
    int delay = resourcePermitStore().getPermitRetryDelay("missing-resource");

    assertEquals(5000, delay, "Unknown resources should use the default retry delay");
  }

  @Test
  void tryAcquirePermit_concurrent_respectsCapacity() {
    int capacity = 3;
    resourcePermitStore().configureResource("res-cap", capacity, 1000, "capacity test");

    AtomicInteger successCount = new AtomicInteger();
    int threadCount = 6;
    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final var job = persist(newPendingJob());
      final String nodeId = "node-" + i;
      tasks[i] =
          () -> {
            if (resourcePermitStore().tryAcquirePermit("res-cap", job.getId(), nodeId)) {
              successCount.incrementAndGet();
            }
          };
    }

    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    assertTrue(
        successCount.get() <= capacity,
        "at most " + capacity + " permits should be acquired; got " + successCount.get());
    assertTrue(successCount.get() >= 1, "at least one permit should be acquired");
  }

  @Test
  void cleanupOrphanedPermits_removesStaleNodePermits() {
    resourcePermitStore().configureResource("res-orphan", 1, 1000, "orphan test");
    var job = persist(newPendingJob());

    resourcePermitStore().tryAcquirePermit("res-orphan", job.getId(), "stale-node");

    int cleaned = resourcePermitStore().cleanupOrphanedPermits(List.of("stale-node"));

    assertTrue(cleaned >= 1, "cleanupOrphanedPermits should remove the stale permit");

    var otherJob = persist(newPendingJob());
    assertTrue(
        resourcePermitStore().tryAcquirePermit("res-orphan", otherJob.getId(), "node-1"),
        "Resource should be freed after orphan cleanup");
  }

  @Test
  void configureResource_updatesExistingConfig() {
    resourcePermitStore().configureResource("res-reconfig", 1, 1000, "initial");
    resourcePermitStore().configureResource("res-reconfig", 5, 2000, "updated");

    assertEquals(
        2000,
        resourcePermitStore().getPermitRetryDelay("res-reconfig"),
        "Reconfigured resource should have updated retry delay");
  }

  @Test
  void tryAcquirePermit_sameJobTwice_idempotent() {
    resourcePermitStore().configureResource("res-idem", 1, 1000, "idempotent test");
    var job = persist(newPendingJob());

    boolean first = resourcePermitStore().tryAcquirePermit("res-idem", job.getId(), "node-1");
    boolean second = resourcePermitStore().tryAcquirePermit("res-idem", job.getId(), "node-1");

    assertTrue(first, "First acquire should succeed");
    assertTrue(second, "Second acquire for the same job should be idempotent");

    var otherJob = persist(newPendingJob());
    assertFalse(
        resourcePermitStore().tryAcquirePermit("res-idem", otherJob.getId(), "node-1"),
        "Resource capacity should remain exhausted by the one idempotent permit");
  }
}
