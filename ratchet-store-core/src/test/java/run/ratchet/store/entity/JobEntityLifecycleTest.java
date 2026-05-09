package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;

class JobEntityLifecycleTest {

  @Test
  void prePersistRejectsMissingRequiredFieldsBeforeDatabaseFlush() {
    JobEntity job = requiredJob();
    job.setScheduledTime(null);

    IllegalStateException ex = assertThrows(IllegalStateException.class, job::prePersist);

    assertTrue(ex.getMessage().contains("scheduledTime"));
  }

  @Test
  void preUpdateRejectsMissingRequiredFieldsBeforeDatabaseFlush() {
    JobEntity job = requiredJob();
    job.setPayload(null);

    IllegalStateException ex = assertThrows(IllegalStateException.class, job::preUpdate);

    assertTrue(ex.getMessage().contains("payload"));
  }

  @Test
  void lifecycleCallbacksKeepTimestampsForValidJob() {
    JobEntity job = requiredJob();

    job.prePersist();
    assertNotNull(job.getCreatedAt());
    assertNotNull(job.getUpdatedAt());

    job.preUpdate();
    assertNotNull(job.getUpdatedAt());
  }

  private static JobEntity requiredJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.parse("2026-05-07T12:00:00Z"));
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setCronExpr("");
    job.setZoneId("UTC");
    job.setPayload(new JobPayload("com.example.Job", "run", "()V", false, List.of()));
    job.setIdempotencyKey("idem-1");
    return job;
  }
}
