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
package run.ratchet.ri.core;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.ResourceLimit;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

/**
 * SPI for the distributed semaphore that allows N concurrent job holders per named resource.
 * Default implementation: {@link run.ratchet.ri.core.internal.DefaultResourcePermitService}.
 *
 * @apiNote Framework SPI consumed by ri.core.JobTask / OrphanRecoveryTimer and by ratchet-testsuite
 *     integration tests. Applications must not implement this interface.
 */
public interface ResourcePermitService {

  /**
   * Attempts to acquire a permit atomically using pessimistic locking.
   *
   * <p>The capacity check is atomic, but the claim check is point in time. A claim change that
   * commits after it does not stop the write, so the earlier claim can still get ACQUIRED and holds
   * the permit until it releases it. A capacity miss parks the claim in the store without spending
   * an attempt; the worker must not reschedule it again.
   *
   * @return ACQUIRED, AT_CAPACITY, or SUPERSEDED when the claim did not own the job at the check
   */
  AcquireResult tryAcquire(String resourceName, UUID jobId, long claimSeq, String nodeId);

  /** Releases a permit held by the supplied claim. Safe to call even if the job holds no permit. */
  void release(String resourceName, UUID jobId, long claimSeq);

  /** Releases all permits held by a job. */
  void releaseAll(UUID jobId);

  /**
   * @return delay in milliseconds, or 5000 as default if resource not found
   */
  int getRetryDelay(String resourceName);

  /**
   * Configures a resource and wakes waiters after commit.
   *
   * @throws UnsupportedOperationException when there is no ResourcePermitStore
   */
  void configureResource(
      String resourceName, int maxConcurrent, int retryDelayMs, String description);

  /** The stored limit; empty when there is none or no ResourcePermitStore. */
  Optional<ResourceLimit> findResourceLimit(String resourceName);

  /**
   * Every stored limit sorted by name in String natural order, unmodifiable; empty with no store.
   */
  List<ResourceLimit> listResourceLimits();

  /** Wakes resource waiters and removes stale waiter rows with bounded work. */
  void sweepWaiters();

  /** Call periodically, e.g. during node heartbeat. */
  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
