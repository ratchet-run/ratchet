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

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobsBulkCancelledEvent;

/**
 * Base contract for the bulk cancel-by-tag event semantics.
 *
 * <p>{@code cancelJobsByTag} MUST publish exactly one {@link JobsBulkCancelledEvent} carrying the
 * tag and the cancelled count, and MUST NOT fire a per-job {@link JobCancelledEvent} for the jobs
 * it cancels. This keeps a kill-switch teardown from flooding observers with one event per job.
 */
public abstract class AbstractBulkCancelEventContract {

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
  void cancelJobsByTag_publishesOneBulkEventAndNoPerJobEvents() {
    String tag = "bulk-cancel-tck-" + UUID.randomUUID();
    waitingTaggedJob(tag);
    waitingTaggedJob(tag);
    waitingTaggedJob(tag);

    // Register only now so creation/submit events are not captured; the cancel path fires
    // synchronously through the listener bridge before cancelJobsByTag returns.
    listener = events::add;
    runtime().scheduler().addEventListener(listener);

    int cancelled = runtime().scheduler().cancelJobsByTag(tag);
    assertEquals(3, cancelled, "cancelJobsByTag must report every job it cancelled");

    List<JobsBulkCancelledEvent> bulk =
        events.stream()
            .filter(JobsBulkCancelledEvent.class::isInstance)
            .map(JobsBulkCancelledEvent.class::cast)
            .filter(e -> tag.equals(e.getTag()))
            .toList();
    assertEquals(
        1,
        bulk.size(),
        "exactly one JobsBulkCancelledEvent must be published for the cancelled tag");
    assertEquals(3, bulk.get(0).getCount(), "the bulk event count must match the jobs cancelled");

    long perJob = events.stream().filter(JobCancelledEvent.class::isInstance).count();
    assertEquals(
        0, perJob, "bulk cancel-by-tag must not fire a per-job JobCancelledEvent for its jobs");
  }

  private void waitingTaggedJob(String tag) {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::noop)
            .withTags(tag)
            .awaitSignal("bulk-cancel-never-" + UUID.randomUUID(), Duration.ofMinutes(5))
            .submit();
    runtime().probe().track(handle);
  }

  protected abstract RatchetTckRuntime runtime();
}
