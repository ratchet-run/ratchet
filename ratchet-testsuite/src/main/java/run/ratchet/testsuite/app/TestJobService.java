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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.UUID;
import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.StreamingBatchBuilder;

@ApplicationScoped
public class TestJobService {

  @Inject private JobSchedulerService scheduler;

  public JobBuilder enqueue(SerializableCheckedRunnable task) {
    return scheduler.enqueue(task);
  }

  public JobHandle enqueueNow(SerializableCheckedRunnable task) {
    return scheduler.enqueueNow(task);
  }

  public JobBuilder schedule(Duration delay, SerializableCheckedRunnable task) {
    return scheduler.schedule(delay, task);
  }

  public boolean cancelJob(UUID jobId) {
    return scheduler.cancelJob(jobId);
  }

  public boolean pauseJob(UUID jobId) {
    return scheduler.pauseJob(jobId);
  }

  public boolean resumeJob(UUID jobId) {
    return scheduler.resumeJob(jobId);
  }

  public boolean retryJob(UUID jobId) {
    return scheduler.retryJob(jobId);
  }

  public BatchBuilder enqueueBatch(String name) {
    return scheduler.enqueueBatch(name);
  }

  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return scheduler.streamingBatch(name);
  }

  public RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task) {
    return scheduler.scheduleRecurring(cron, zone, task);
  }

  public JobSchedulerService getScheduler() {
    return scheduler;
  }
}
