package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

/** Claims due recurring masters, spawns child jobs, and advances next-fire times. */
@ApplicationScoped
@Transactional
public class RecurringJobExecutor {

  private static final Logger log = Logger.getLogger(RecurringJobExecutor.class);

  // Caps catch-up executions per recurring job to prevent thundering herd after downtime.
  private static final int MAX_CATCHUP_COUNT = 10;

  private final JobCrudStore jobCrudStore;
  private final JobClaimStore jobClaimStore;
  private final JobTerminalStore jobTerminalStore;
  private final RecurringRegistrationState registrationState;
  private final NodeTagAffinityProvider tagAffinityProvider;

  protected RecurringJobExecutor() {
    this.jobCrudStore = null;
    this.jobClaimStore = null;
    this.jobTerminalStore = null;
    this.registrationState = null;
    this.tagAffinityProvider = null;
  }

  @Inject
  public RecurringJobExecutor(
      JobCrudStore jobCrudStore,
      JobClaimStore jobClaimStore,
      JobTerminalStore jobTerminalStore,
      RecurringRegistrationState registrationState,
      NodeTagAffinityProvider tagAffinityProvider) {
    this.jobCrudStore = jobCrudStore;
    this.jobClaimStore = jobClaimStore;
    this.jobTerminalStore = jobTerminalStore;
    this.registrationState = registrationState;
    this.tagAffinityProvider = tagAffinityProvider;
  }

  void enqueueChild(JobEntity master, Instant fireTs) {
    JobEntity child = createChildFromMaster(master, fireTs);
    jobCrudStore.save(child);
  }

  /**
   * Claims due recurring masters, creates child jobs, advances each master's next-fire timestamp,
   * and returns the number of children scheduled.
   *
   * <p>Runs with the class-level Jakarta Transactions {@code REQUIRED} behavior.
   */
  int process(int batchLimit, String nodeId) {
    NodeTagFilter tagFilter =
        tagAffinityProvider != null ? tagAffinityProvider.tagFilter() : NodeTagFilter.NONE;
    List<JobEntity> masters = jobClaimStore.claimDueRecurring(batchLimit, nodeId, tagFilter);
    Instant now = Instant.now();
    int firedCount = 0;
    for (JobEntity master : masters) {
      // Startup grace gate: during the first ratchet.recurring.startup-grace-seconds after this
      // node finished its @Recurring registration pass, refuse to fire any master whose business
      // key is not in the local known-keys set. This closes the rolling-deploy race where Node
      // B's newer JAR removes an annotation but Node A (or even Node B itself, before cleanup
      // runs) might claim and fire the orphaned master between scan cycles. Post hot/cold-split,
      // recurring masters have no hot row and no picked_by/picked_at to clear — releasing means
      // letting the FOR UPDATE SKIP LOCKED row lock drop at tx end. We just continue without
      // advancing next_fire so the master is eligible next cycle.
      if (registrationState != null && !registrationState.shouldFire(master.getBusinessKey())) {
        log.debugf(
            "Recurring master %s (businessKey=%s) skipped — within startup grace and key not"
                + " in local known set",
            master.getId(), master.getBusinessKey());
        continue;
      }
      Cron cron;
      ZoneId zone;
      try {
        cron = RecurringScheduler.PARSER.parse(master.getCronExpr());
        zone = ZoneId.of(master.getZoneId());
      } catch (RuntimeException e) {
        log.warnf(e, "Recurring job %s skipped after scheduling error", master.getId());
        continue;
      }
      ExecutionTime execTime = ExecutionTime.forCron(cron);

      Instant baseTime = master.getNextFire() != null ? master.getNextFire() : now;

      enqueueChild(master, baseTime.isBefore(now) ? baseTime : now);
      firedCount++;

      Optional<Instant> nextOpt =
          execTime.nextExecution(baseTime.atZone(zone)).map(ZonedDateTime::toInstant);

      int catchupCount = 0;
      while (nextOpt.isPresent()
          && nextOpt.get().isBefore(now)
          && catchupCount < MAX_CATCHUP_COUNT) {
        enqueueChild(master, nextOpt.get());
        catchupCount++;
        nextOpt = execTime.nextExecution(nextOpt.get().atZone(zone)).map(ZonedDateTime::toInstant);
      }

      if (catchupCount > 0) {
        log.infof(
            "Recurring job %s caught up on %s missed executions", master.getId(), catchupCount);
      }

      while (nextOpt.isPresent() && nextOpt.get().isBefore(now)) {
        nextOpt = execTime.nextExecution(nextOpt.get().atZone(zone)).map(ZonedDateTime::toInstant);
      }

      if (nextOpt.isPresent()) {
        // Cold-only metadata UPDATE — save() is allowed for next_fire advance.
        master.setNextFire(nextOpt.get());
        master.setStatus(JobStatus.PENDING);
        jobCrudStore.save(master);
      } else {
        // Cron exhausted — explicit cancel pathway runs the recurring-cancel SQL atomically
        // (clear rec_status, set terminal_status='CANCELED', drop bkres). Routing through
        // save() would trip the hot-mutation guard since save() can't represent the rec_status
        // / terminal_status transition.
        jobTerminalStore.cancelJob(master.getId());
      }
      log.infof("Recurring job %s fired; next=%s", master.getId(), master.getNextFire());
    }
    return firedCount;
  }

  private JobEntity createChildFromMaster(JobEntity master, Instant fireTs) {
    JobEntity child = new JobEntity();
    child.setPayload(master.getPayload());
    child.setJobType(JobExecutionType.SINGLE);
    child.setStatus(JobStatus.PENDING);
    child.setScheduledTime(fireTs);
    child.setPriority(master.getPriority());
    child.setMaxRetries(master.getMaxRetries());
    child.setBackoffPolicy(master.getBackoffPolicy());
    child.setBackoffParamMs(master.getBackoffParamMs());
    child.setTimeoutSec(master.getTimeoutSec());
    child.setDependsOn(master.getId());
    child.setIdempotencyKey(UUID.randomUUID().toString());
    return child;
  }
}
