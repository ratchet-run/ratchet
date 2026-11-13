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
package run.ratchet.store.spi;

import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.Nullable;
import run.ratchet.api.exception.RatchetTransientStoreException;

/** Resource permit management operations for concurrency limiting. */
@Incubating
public interface ResourcePermitStore {

  /** A parked waiter that a wake-up made due again. */
  record WokenWaiter(UUID jobId, int priority, @Nullable String executionTarget) {}

  /**
   * Makes up to the free capacity of live head waiters due again, setting each one's due time back
   * to when it started waiting. Live waiters that are already running use up capacity but are not
   * returned. Lease times are never renewed here.
   *
   * <p>The wake never waits on a job row that another transaction holds. A store with row locks
   * skips that head and does not return it. A skipped head that is still waiting becomes due at its
   * own retry time, at a later wake-up or at the sweep, so its share of capacity can stay unused
   * until then. A store whose transactions abort on a write conflict retries the whole wake
   * instead.
   *
   * <p>Transaction attribute: {@code REQUIRES_NEW}. The resource lock must be the first lock of the
   * transaction, and the release or clean-up that freed the capacity must already be committed.
   */
  List<WokenWaiter> wakeWaiters(String resource);

  /**
   * Up to {@code limit} distinct resource names that have waiter rows, in name order. One indexed
   * read. Transaction attribute: {@code SUPPORTS}.
   */
  List<String> findResourcesWithWaiters(int limit);

  /**
   * Deletes up to {@code limit} waiter rows whose job has no queue row, or whose queue status is
   * not PENDING, RUNNING or PAUSED. Returns the number deleted. Transaction attribute: {@code
   * REQUIRES_NEW}.
   */
  int deleteStaleWaiters(int limit);

  /** Outcome of {@link #tryAcquirePermit(String, UUID, long, String)}. */
  enum AcquireResult {
    /** The claim holds the permit. */
    ACQUIRED,
    /** The claim was parked because capacity or earlier live waiters blocked it. */
    AT_CAPACITY,
    /** The claim did not own the running job when the store checked it. */
    SUPERSEDED
  }

  /**
   * Attempts to acquire capacity for a configured resource.
   *
   * <p>The resource lock is the first lock in this transaction. An unknown resource throws before
   * the non-locking, point-in-time RUNNING/claim check. A superseded claim writes nothing. A claim
   * change after this check may still allow an available permit to be acquired. The same claim
   * acquires idempotently; older claims' permits still consume capacity.
   *
   * <p>Live waiters are ordered by priority descending, original due time ascending, then job id
   * ascending. A waiter is live only while its lease is current and its queue status is PENDING or
   * RUNNING. Available capacity is granted only when fewer live waiters are ahead than slots free.
   * Success removes the caller's waiter row. Otherwise the store parks the job atomically at the
   * clock its claim compares against plus retry delay (PAUSED in a paused batch), preserving
   * attempts and its first wait key, and renews the lease. A fenced park miss returns SUPERSEDED
   * and parks nothing.
   *
   * @param resource configured resource name to acquire against; never {@code null}
   * @param jobId job id the permit is being acquired for; never {@code null}
   * @param claimSeq claim that must own the running job when the store checks it
   * @param nodeId stable identity of the acquiring node; never {@code null} or blank
   * @return whether the claim acquired capacity, must wait, or did not own the job at the check
   * @throws IllegalArgumentException when {@code resource} has not been configured
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  AcquireResult tryAcquirePermit(String resource, UUID jobId, long claimSeq, String nodeId);

  /**
   * Releases only the permit belonging to the supplied resource, job and claim. Missing permits are
   * a no-op. A permit that another claim of the same job holds stays in place.
   *
   * <p>Does not wake waiters. The engine calls {@link #wakeWaiters} in a separate transaction after
   * commit, keeping the resource lock first.
   *
   * @param resource configured resource name to release against; never {@code null}
   * @param jobId job id that owns the permit; never {@code null}
   * @param claimSeq claim that owns the permit
   * @throws RatchetTransientStoreException if the backing store cannot complete the release
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  void releasePermit(String resource, UUID jobId, long claimSeq);

  /**
   * Releases all permits for one job. Unknown jobs are a no-op.
   *
   * <p>Does not wake waiters. After commit, the engine calls {@link #wakeWaiters} in a separate
   * transaction so its resource lock is first.
   *
   * @param jobId job id whose permits should be released; never {@code null}
   * @throws RatchetTransientStoreException if the backing store cannot complete the release
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  void releaseAllPermits(UUID jobId);

  /**
   * Reads the retry delay for a resource.
   *
   * @param resource configured resource name to query
   * @return the configured delay, or the store default delay when {@code resource} is unknown
   *     <p>Transaction attribute: {@code SUPPORTS}.
   */
  int getPermitRetryDelay(String resource);

  /**
   * Creates or updates a resource limit. Transaction attribute: {@code REQUIRED}.
   *
   * <p>Does not wake waiters. The engine calls {@link #wakeWaiters} after commit in a separate
   * transaction, keeping the resource lock first.
   *
   * @param name resource name (primary key); never {@code null} or blank
   * @param maxConcurrent maximum concurrent permits allowed for the resource; must be positive
   * @param retryDelayMs delay in milliseconds before a caller may retry after a permit miss; must
   *     be non-negative
   * @param description free-form description for operator dashboards, or {@code null} to omit
   */
  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description);

  /**
   * Removes permits by the node id they carry. Transaction attribute: {@code REQUIRED}.
   *
   * <p>Does not wake waiters. The engine calls {@link #wakeWaiters} after commit in a separate
   * transaction, keeping the resource lock first.
   *
   * @param staleNodeIds node ids whose permits should be released; never {@code null}, may be empty
   *     (no-op when empty)
   * @return number of permit rows released
   */
  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
