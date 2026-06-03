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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Base contract tests for {@code JobAnalyticsStore} (aggregate counts and grouped metrics). */
public abstract class AbstractJobAnalyticsStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupAnalyticsFixture() {
    cleanupStore();
  }

  @Test
  void countJobsByStatuses_returnsGroupedStatusCounts() {
    persist(newPendingJob());

    var running = persist(newPendingJob());
    store().compareAndSwapStatus(running.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    var succeeded = persist(newPendingJob());
    store().compareAndSwapStatus(succeeded.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(succeeded.getId(), null, null, Instant.now(), Instant.now(), 0L, 0L);

    Map<JobStatus, Long> counts = analyticsStore().countJobsByStatuses();

    assertEquals(1L, counts.get(JobStatus.PENDING));
    assertEquals(1L, counts.get(JobStatus.RUNNING));
    assertEquals(1L, counts.get(JobStatus.SUCCEEDED));
  }

  @Test
  void countPendingJobsByPriorities_returnsGroupedPendingCounts() {
    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    persist(high);

    JobEntity critical = newPendingJob();
    critical.setPriority(JobPriority.CRITICAL);
    persist(critical);

    JobEntity running = newPendingJob();
    running.setPriority(JobPriority.HIGH);
    JobEntity savedRunning = persist(running);
    store().compareAndSwapStatus(savedRunning.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Map<JobPriority, Long> counts = analyticsStore().countPendingJobsByPriorities();

    assertEquals(1L, counts.get(JobPriority.HIGH));
    assertEquals(1L, counts.get(JobPriority.CRITICAL));
  }

  @Test
  void countPendingJobsByTypes_returnsGroupedPendingCounts() {
    JobEntity single = newPendingJob();
    single.setJobType(JobExecutionType.SINGLE);
    persist(single);

    JobEntity child = newPendingJob();
    child.setJobType(JobExecutionType.BATCH_CHILD);
    persist(child);

    JobEntity running = newPendingJob();
    running.setJobType(JobExecutionType.SINGLE);
    JobEntity savedRunning = persist(running);
    store().compareAndSwapStatus(savedRunning.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Map<JobExecutionType, Long> counts = analyticsStore().countPendingJobsByTypes();

    assertEquals(1L, counts.get(JobExecutionType.SINGLE));
    assertEquals(1L, counts.get(JobExecutionType.BATCH_CHILD));
  }

  @Test
  void countJobsByParamForTag_supportsLiteralParamKeysWithDots() {
    var first = newPendingJob();
    first.setParams(Map.of("loadtest.enqueue.node", "node-a"));
    first = persist(first);
    store().insertTags(first.getId(), List.of("run-tag"));

    var second = newPendingJob();
    second.setParams(Map.of("loadtest.enqueue.node", "node-a"));
    second = persist(second);
    store().insertTags(second.getId(), List.of("run-tag"));

    var third = newPendingJob();
    third.setParams(Map.of("loadtest.enqueue.node", "node-b"));
    third = persist(third);
    store().insertTags(third.getId(), List.of("run-tag"));

    Map<String, Long> counts =
        analyticsStore().countJobsByParamForTag("run-tag", "loadtest.enqueue.node");

    assertEquals(2L, counts.get("node-a"));
    assertEquals(1L, counts.get("node-b"));
  }

  @Test
  void countJobsByStatusForTag_groupsOnlyTaggedJobs() {
    var pending = newPendingJob();
    pending.setStatus(JobStatus.PENDING);
    pending = persist(pending);
    store().insertTags(pending.getId(), List.of("run-tag"));

    var running = newPendingJob();
    running.setStatus(JobStatus.RUNNING);
    running = persist(running);
    store().insertTags(running.getId(), List.of("run-tag"));

    var secondRunning = newPendingJob();
    secondRunning.setStatus(JobStatus.RUNNING);
    secondRunning = persist(secondRunning);
    store().insertTags(secondRunning.getId(), List.of("run-tag"));

    var otherTag = newPendingJob();
    otherTag.setStatus(JobStatus.FAILED);
    otherTag = persist(otherTag);
    store().insertTags(otherTag.getId(), List.of("other-tag"));

    Map<JobStatus, Long> counts = analyticsStore().countJobsByStatusForTag("run-tag");

    assertEquals(Map.of(JobStatus.PENDING, 1L, JobStatus.RUNNING, 2L), counts);
  }

  @Test
  void countJobsByExecutionNodeForTag_groupsOnlyTaggedJobsWithNodes() {
    var first = newPendingJob();
    first.setPickedBy("node-a");
    first = persist(first);
    store().insertTags(first.getId(), List.of("run-tag"));

    var second = newPendingJob();
    second.setPickedBy("node-a");
    second = persist(second);
    store().insertTags(second.getId(), List.of("run-tag"));

    var third = newPendingJob();
    third.setPickedBy("node-b");
    third = persist(third);
    store().insertTags(third.getId(), List.of("run-tag"));

    var unassigned = newPendingJob();
    unassigned = persist(unassigned);
    store().insertTags(unassigned.getId(), List.of("run-tag"));

    var otherTag = newPendingJob();
    otherTag.setPickedBy("node-c");
    otherTag = persist(otherTag);
    store().insertTags(otherTag.getId(), List.of("other-tag"));

    Map<String, Long> counts = analyticsStore().countJobsByExecutionNodeForTag("run-tag");

    assertEquals(Map.of("node-a", 2L, "node-b", 1L), counts);
  }
}
