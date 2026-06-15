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
package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Metrics collector that records callback payloads for integration assertions. */
@Alternative
@Priority(3)
@ApplicationScoped
public class RecordingMetricsCollector extends TestMetricsCollectorAdapter {

  private static final ConcurrentLinkedQueue<StartedMetric> STARTED = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<CompletedMetric> COMPLETED =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<FailedMetric> FAILED = new ConcurrentLinkedQueue<>();

  public static List<StartedMetric> startedEvents() {
    return List.copyOf(STARTED);
  }

  public static List<CompletedMetric> completedEvents() {
    return List.copyOf(COMPLETED);
  }

  public static List<FailedMetric> failedEvents() {
    return List.copyOf(FAILED);
  }

  public static void reset() {
    STARTED.clear();
    COMPLETED.clear();
    FAILED.clear();
  }

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
    STARTED.add(new StartedMetric(jobId, type, priority));
  }

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
    COMPLETED.add(new CompletedMetric(jobId, type, executionTimeMs));
  }

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    FAILED.add(
        new FailedMetric(jobId, type, attempt, cause == null ? null : cause.getClass().getName()));
  }

  public record StartedMetric(UUID jobId, JobType type, JobPriority priority) {}

  public record CompletedMetric(UUID jobId, JobType type, long executionTimeMs) {}

  public record FailedMetric(UUID jobId, JobType type, int attempt, String causeType) {}
}
