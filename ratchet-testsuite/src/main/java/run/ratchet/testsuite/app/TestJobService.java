package run.ratchet.testsuite.app;

import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.StreamingBatchBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;

/**
 * CDI bean wrapping {@link JobSchedulerService} for test convenience.
 *
 * <p>Provides simplified methods for common test operations and verifies CDI injection is working
 * correctly.
 */
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

  public boolean cancelJob(long jobId) {
    return scheduler.cancelJob(jobId);
  }

  public boolean pauseJob(long jobId) {
    return scheduler.pauseJob(jobId);
  }

  public boolean resumeJob(long jobId) {
    return scheduler.resumeJob(jobId);
  }

  public boolean retryJob(long jobId) {
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
