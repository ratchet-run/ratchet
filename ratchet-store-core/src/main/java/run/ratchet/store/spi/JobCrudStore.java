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
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;
import run.ratchet.store.entity.JobEntity;

/** Core CRUD and primary-key lookup operations for jobs. */
@Incubating
public interface JobCrudStore {

  int DEFAULT_PAGE_LIMIT = 100;

  /**
   * Inserts a new job row and returns the persisted entity view.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   */
  JobEntity create(JobEntity job);

  /**
   * Updates an existing job row and returns the persisted entity view.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   */
  JobEntity save(JobEntity job);

  /** Finds a job by primary key. Transaction attribute: {@code SUPPORTS}. */
  Optional<JobEntity> findById(UUID id);

  /**
   * Loads the latest job row by primary key. Despite the method name, no row-level lock is acquired
   * — backends rely on optimistic version checks at the actual mutation site ({@code
   * findOneAndUpdate} on Mongo, {@code WHERE version = ?} on SQL). Callers MUST use a
   * version-checked update path; this method is read-only.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  Optional<JobEntity> findByIdLatest(UUID id);

  /** Deletes a job row by primary key. Transaction attribute: {@code REQUIRED}. */
  void delete(UUID id);

  /**
   * Returns the current persisted status for a job.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @apiNote This method intentionally returns a bare reference rather than {@link Optional} so the
   *     hot status-check path avoids one allocation per call. Callers must null-check the result;
   *     the {@link Nullable} annotation reflects this contract for static analysers.
   * @param id job id to inspect
   * @return current status, or {@code null} when no job exists for {@code id}
   * @throws IllegalStateException if a job row exists but carries neither a live nor a terminal
   *     status. This is a store invariant violation; every store fails loud here rather than
   *     returning {@code null} (which a caller could not distinguish from "no such job").
   */
  @Nullable JobStatus getJobStatus(UUID id);

  /**
   * Batch-loads jobs by primary key for hot-path recovery and draining flows.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobEntity> findByIds(List<UUID> ids);

  /**
   * Finds the active job currently associated with a business key, if any.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @throws IllegalStateException if a reservation claims a live-queue owner but no hot row exists.
   *     This is a store invariant violation; every store fails loud here rather than returning an
   *     empty {@link Optional}.
   */
  Optional<JobEntity> findActiveByBusinessKey(String businessKey);

  /** Finds a job by idempotency key. Transaction attribute: {@code SUPPORTS}. */
  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

  /**
   * Returns a page of direct dependant jobs whose {@code dependsOn} points at the supplied parent.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset);

  /**
   * Counts pending jobs. Consulted by the engine's dynamic-heartbeat backpressure loop, so it stays
   * on the core contract rather than the optional analytics capability.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  long countPendingJobs();

  /**
   * Counts currently registered scheduler nodes. Consulted by the engine's dynamic-heartbeat loop,
   * so it stays on the core contract rather than the optional analytics capability.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  long countActiveNodes();
}
