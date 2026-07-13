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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobsBulkRetriedEvent;

/** Public-API contract for bounded, filtered recovery of jobs in the dead-letter queue. */
public abstract class AbstractBulkRetryContract {

  private final List<Object> events = new CopyOnWriteArrayList<>();
  private Consumer<Object> listener;

  @AfterEach
  void clearAfterEach() {
    if (listener != null) {
      runtime().scheduler().removeEventListener(listener);
      listener = null;
    }
    runtime().clear();
    TckJobs.resetAll();
    events.clear();
  }

  @Test
  void retryJobs_filtersFailedJobsAndHonorsTheExplicitLimit() throws InterruptedException {
    String matchingTag = "bulk-retry-tck-" + UUID.randomUUID();
    JobHandle first = failingJob(matchingTag);
    JobHandle second = failingJob(matchingTag);
    JobHandle other = failingJob("bulk-retry-other-" + UUID.randomUUID());

    assertTrue(runtime().probe().awaitFailed(first, defaultTimeout()));
    assertTrue(runtime().probe().awaitFailed(second, defaultTimeout()));
    assertTrue(runtime().probe().awaitFailed(other, defaultTimeout()));

    JobFilter filter = JobFilter.builder().tags(matchingTag).build();
    listener = events::add;
    runtime().scheduler().addEventListener(listener);

    int retried = runtime().scheduler().retryJobs(filter, 1);

    assertEquals(1, retried, "one bounded call must recover at most one matching failed job");
    List<JobsBulkRetriedEvent> bulkEvents =
        events.stream()
            .filter(JobsBulkRetriedEvent.class::isInstance)
            .map(JobsBulkRetriedEvent.class::cast)
            .toList();
    assertEquals(1, bulkEvents.size(), "bulk recovery must publish one aggregate event");
    assertEquals(filter, bulkEvents.get(0).getFilter());
    assertEquals(1, bulkEvents.get(0).getLimit());
    assertEquals(1, bulkEvents.get(0).getCount());
    assertEquals(
        0,
        events.stream().filter(JobRetryingEvent.class::isInstance).count(),
        "bulk recovery must not flood observers with per-job retry events");
    awaitInvocationCount(first, second, 3);
    assertEquals(
        1,
        runtime().probe().invocationCount(other),
        "a failed job outside the filter must not be retried");
  }

  @Test
  void retryJobs_rejectsLimitsOutsideThePortableBound() {
    JobFilter failedJobs = JobFilter.builder().build();

    assertThrows(NullPointerException.class, () -> runtime().scheduler().retryJobs(null, 1));
    assertThrows(
        IllegalArgumentException.class, () -> runtime().scheduler().retryJobs(failedJobs, 0));
    assertThrows(
        IllegalArgumentException.class, () -> runtime().scheduler().retryJobs(failedJobs, 1001));
  }

  private JobHandle failingJob(String tag) {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::throwIntentional)
            .withMaxRetries(0)
            .withTags(tag)
            .submit();
    runtime().probe().track(handle);
    return handle;
  }

  private void awaitInvocationCount(JobHandle first, JobHandle second, int expected)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(defaultTimeout());
    while (Instant.now().isBefore(deadline)) {
      int total =
          runtime().probe().invocationCount(first) + runtime().probe().invocationCount(second);
      if (total >= expected) {
        return;
      }
      Thread.sleep(50L);
    }
    assertEquals(
        expected,
        runtime().probe().invocationCount(first) + runtime().probe().invocationCount(second),
        "the recovered job must be invoked again");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }
}
