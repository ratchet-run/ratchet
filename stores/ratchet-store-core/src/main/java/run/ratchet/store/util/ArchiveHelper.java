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
package run.ratchet.store.util;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/** Shared utilities for archiving jobs across JPA-based store implementations. */
public final class ArchiveHelper {

  /** JPQL for finding terminal jobs older than a cutoff, used by both JPA store implementations. */
  // language=JPAQL
  public static final String FIND_JOBS_FOR_ARCHIVING_JPQL =
      """
      SELECT DISTINCT j FROM JobEntity j LEFT JOIN FETCH j.tags
      WHERE j.status IN (
        run.ratchet.api.JobStatus.SUCCEEDED,
        run.ratchet.api.JobStatus.FAILED,
        run.ratchet.api.JobStatus.CANCELED)
        AND j.updatedAt < :cutoff
      ORDER BY j.updatedAt ASC
      """;

  private ArchiveHelper() {}

  /**
   * Orders freshly loaded jobs to match a requested batch and rejects stale/nonterminal members.
   */
  public static List<JobEntity> requireCurrentTerminalJobs(
      List<UUID> requestedIds, List<JobEntity> currentJobs) {
    Objects.requireNonNull(requestedIds, "requestedIds");
    Objects.requireNonNull(currentJobs, "currentJobs");

    Set<UUID> uniqueIds = new HashSet<>(requestedIds);
    if (uniqueIds.size() != requestedIds.size()) {
      throw new IllegalArgumentException("Archive batch job IDs must be distinct");
    }

    Map<UUID, JobEntity> currentById = new HashMap<>(currentJobs.size());
    for (JobEntity current : currentJobs) {
      currentById.put(current.getId(), current);
    }

    List<JobEntity> ordered = new ArrayList<>(requestedIds.size());
    for (UUID id : requestedIds) {
      JobEntity current = currentById.get(id);
      if (current == null || current.getStatus() == null || !current.getStatus().isTerminal()) {
        throw new RatchetTransientStoreException(
            "Archive batch contains a missing or nonterminal job " + id + "; rolling back");
      }
      ordered.add(current);
    }
    return ordered;
  }

  /** Verifies the terminal-guarded delete moved every requested job. */
  public static int requireAllDeleted(List<UUID> requestedIds, int deleted) {
    if (deleted != requestedIds.size()) {
      throw new RatchetTransientStoreException(
          "Archive batch raced a status change: archived "
              + requestedIds.size()
              + " but deleted "
              + deleted
              + "; rolling back");
    }
    return deleted;
  }

  /**
   * Populates an {@link ArchivedJobEntity} from the given job, reason, archivedBy, and a
   * caller-supplied timestamp.
   *
   * <p>Callers that have access to a {@link Clock} (e.g. the RI archival service) should pass
   * {@code clock.instant()} so the archive timestamp is testable and deterministic. Callers without
   * clock access may use the convenience overload {@link #buildArchive(JobEntity, String, String)},
   * which falls back to {@link Instant#now()}.
   */
  public static ArchivedJobEntity buildArchive(
      JobEntity job, String reason, String archivedBy, Instant archivedAt) {
    Objects.requireNonNull(job, "job");
    Objects.requireNonNull(archivedAt, "archivedAt");
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setOriginalJobId(job.getId());
    a.setFinalStatus(job.getStatus());
    a.setJobType(job.getJobType());
    a.setPriority(job.getPriority());
    a.setTotalAttempts(job.getAttempts());
    a.setMaxRetries(job.getMaxRetries());
    a.setBackoffPolicy(job.getBackoffPolicy());
    a.setBackoffParamMs(job.getBackoffParamMs());
    a.setTimeoutSec(job.getTimeoutSec());
    a.setTargetClass(job.getTargetClass());
    a.setMethodName(job.getMethodName());
    a.setBusinessKey(job.getBusinessKey());
    a.setCronExpr(job.getCronExpr());
    a.setZoneId(job.getZoneId());
    a.setOriginalScheduledTime(job.getScheduledTime());
    a.setOriginalCreatedAt(job.getCreatedAt());
    a.setFirstExecutionTime(job.getExecutionStartTime());
    a.setCompletionTime(job.getExecutionEndTime());
    a.setTotalExecutionTimeMs(job.getExecutionDurationMs());
    a.setQueueWaitMs(job.getQueueWaitMs());
    a.setArchivedAt(archivedAt);
    a.setArchivedBy(archivedBy);
    a.setArchiveReason(reason);
    a.setJobResult(job.getJobResult());
    a.setResultType(job.getResultType());
    a.setFinalError(job.getLastError());
    if (job.getPayload() != null) {
      a.setPayloadSummary(job.getPayload().target() + "#" + job.getPayload().method());
    }
    a.setDependedOn(job.getDependsOn());
    a.setSupersededBy(job.getSupersededBy());
    if (job.getTags() != null && !job.getTags().isEmpty()) {
      a.setTags(String.join(",", job.getTags()));
    }
    return a;
  }

  /**
   * Convenience overload that uses {@link Instant#now()} as the archive timestamp. Prefer {@link
   * #buildArchive(JobEntity, String, String, Instant)} when a {@link java.time.Clock} is available
   * so the timestamp is testable.
   */
  public static ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    return buildArchive(job, reason, archivedBy, Instant.now());
  }
}
