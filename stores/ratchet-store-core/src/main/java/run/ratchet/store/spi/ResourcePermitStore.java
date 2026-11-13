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

/** Resource permit management operations for concurrency limiting. */
@Incubating
public interface ResourcePermitStore {

  /** Outcome of {@link #tryAcquirePermit(String, UUID, long, String)}. */
  enum AcquireResult {
    /** The claim holds the permit. */
    ACQUIRED,
    /** The configured resource has no free capacity. */
    AT_CAPACITY,
    /** The claim did not own the running job when the store checked it. */
    SUPERSEDED
  }

  /**
   * Attempts to acquire capacity for a configured resource.
   *
   * <p>An unconfigured resource throws before checking the claim. The store then checks that the
   * job is RUNNING under the supplied claim, and returns SUPERSEDED without writing anything when
   * it is not. This is a point-in-time check: a claim change that commits after it does not fence
   * the permit write, so the earlier claim can still get ACQUIRED. That claim then holds the permit
   * until it releases it, and the newer claim waits for it.
   *
   * <p>The same claim acquires idempotently. An older claim's permit of the same job counts against
   * capacity like any other permit: when the resource is full, a newer claim gets AT_CAPACITY until
   * that permit is released or cleaned up. A new permit is inserted only when capacity is
   * available.
   *
   * <p>SQL stores that implement this with row locks require the caller's store operation to run in
   * one active transaction so the capacity check and permit insert observe the same locked resource
   * row.
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
   * @param resource configured resource name to release against; never {@code null}
   * @param jobId job id that owns the permit; never {@code null}
   * @param claimSeq claim that owns the permit
   * @throws run.ratchet.api.exception.RatchetTransientStoreException if the backing store cannot
   *     complete the release
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  void releasePermit(String resource, UUID jobId, long claimSeq);

  /**
   * Releases all permits for one job. Unknown jobs are a no-op.
   *
   * @param jobId job id whose permits should be released; never {@code null}
   * @throws run.ratchet.api.exception.RatchetTransientStoreException if the backing store cannot
   *     complete the release
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
   * @param staleNodeIds node ids whose permits should be released; never {@code null}, may be empty
   *     (no-op when empty)
   * @return number of permit rows released
   */
  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
