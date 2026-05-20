package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;

/** Claims due recurring masters, spawns child jobs, and advances next-fire times. */
@ApplicationScoped
@Transactional
public class RecurringJobExecutor {

  private static final Logger log = Logger.getLogger(RecurringJobExecutor.class);

  // Caps catch-up executions per recurring job to prevent thundering herd after downtime.
  private static final int MAX_CATCHUP_COUNT = 10;

  private final JobBulkStore jobBulkStore;
  private final RecurringJobStore recurringJobStore;
  private final RecurringRegistrationState registrationState;
  private final NodeTagAffinityProvider tagAffinityProvider;
  private final Clock clock;

  protected RecurringJobExecutor() {
    this.jobBulkStore = null;
    this.recurringJobStore = null;
    this.registrationState = null;
    this.tagAffinityProvider = null;
    this.clock = null;
  }

  public RecurringJobExecutor(
      JobBulkStore jobBulkStore,
      RecurringJobStore recurringJobStore,
      RecurringRegistrationState registrationState,
      NodeTagAffinityProvider tagAffinityProvider) {
    this(
        jobBulkStore, recurringJobStore, registrationState, tagAffinityProvider, Clock.systemUTC());
  }

  @Inject
  public RecurringJobExecutor(
      JobBulkStore jobBulkStore,
      RecurringJobStore recurringJobStore,
      RecurringRegistrationState registrationState,
      NodeTagAffinityProvider tagAffinityProvider,
      Clock clock) {
    this.jobBulkStore = jobBulkStore;
    this.recurringJobStore = recurringJobStore;
    this.registrationState = registrationState;
    this.tagAffinityProvider = tagAffinityProvider;
    this.clock = clock;
  }

  void enqueueChild(RecurringJobDefinition master, Instant fireTs) {
    JobEntity child = createChildFromMaster(master, fireTs);
    jobBulkStore.bulkInsert(List.of(child));
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
    List<RecurringJobDefinition> masters =
        recurringJobStore.claimDueRecurring(batchLimit, nodeId, tagFilter);
    Instant now = effective().instant();
    List<JobEntity> children = new ArrayList<>();
    int firedCount = 0;
    for (RecurringJobDefinition master : masters) {
      // Startup grace gate: during the first ratchet.recurring.startup-grace-seconds after this
      // node finished its @Recurring registration pass, refuse to fire any master whose business
      // key is not in the local known-keys set. This closes the rolling-deploy race where Node
      // B's newer JAR removes an annotation but Node A (or even Node B itself, before cleanup
      // runs) might claim and fire the orphaned master between scan cycles. SQL stores hold the
      // row lock until tx commit (auto-released on continue); Mongo bumped next_fire forward as
      // its claim lease, so we have to restore the original next_fire explicitly or the master
      // would sit idle for the lease window before becoming eligible again.
      if (registrationState != null && !registrationState.shouldFire(master.businessKey())) {
        log.debugf(
            "Recurring master %s (businessKey=%s) skipped — within startup grace and key not"
                + " in local known set",
            master.id(), master.businessKey());
        releaseClaim(master);
        continue;
      }
      Cron cron;
      ZoneId zone;
      try {
        cron = RecurringScheduler.PARSER.parse(master.cronExpr());
        zone = ZoneId.of(master.zoneId());
      } catch (RuntimeException e) {
        // SQL stores release the row lock on tx commit; Mongo needs the lease undone.
        log.warnf(e, "Recurring job %s skipped after scheduling error", master.id());
        releaseClaim(master);
        continue;
      }
      ExecutionTime execTime = ExecutionTime.forCron(cron);

      Instant baseTime = master.nextFire() != null ? master.nextFire() : now;

      children.add(createChildFromMaster(master, baseTime));
      firedCount++;

      Optional<Instant> nextOpt =
          execTime.nextExecution(baseTime.atZone(zone)).map(ZonedDateTime::toInstant);

      int catchupCount = 0;
      while (nextOpt.isPresent()
          && nextOpt.get().isBefore(now)
          && catchupCount < MAX_CATCHUP_COUNT) {
        children.add(createChildFromMaster(master, nextOpt.get()));
        catchupCount++;
        nextOpt = execTime.nextExecution(nextOpt.get().atZone(zone)).map(ZonedDateTime::toInstant);
      }

      if (catchupCount > 0) {
        log.infof("Recurring job %s caught up on %s missed executions", master.id(), catchupCount);
      }

      if (nextOpt.isPresent() && nextOpt.get().isBefore(now)) {
        nextOpt = execTime.nextExecution(now.atZone(zone)).map(ZonedDateTime::toInstant);
      }

      if (nextOpt.isPresent()) {
        recurringJobStore.advanceNextFire(master.id(), nextOpt.get());
        log.infof("Recurring job %s fired; next=%s", master.id(), nextOpt.get());
      } else {
        // Cron exhausted — atomic archive + live-delete + bkres-cleanup.
        recurringJobStore.cancelRecurringAndArchive(master.id(), ArchiveReason.EXHAUSTED);
        log.infof("Recurring job %s exhausted; archived as EXHAUSTED", master.id());
      }
    }
    if (!children.isEmpty()) {
      jobBulkStore.bulkInsert(children);
    }
    return firedCount;
  }

  /**
   * Restore the master's original next_fire so a skipped row becomes eligible on the next claim
   * cycle. SQL claim implementations don't mutate next_fire so this is effectively a no-op there
   * (the row lock auto-releases on tx commit). Mongo claim bumps next_fire forward as its lease;
   * skipping without releasing would leave the row invisible for the lease window.
   */
  private void releaseClaim(RecurringJobDefinition master) {
    if (master.nextFire() != null) {
      recurringJobStore.advanceNextFire(master.id(), master.nextFire());
    }
  }

  private JobEntity createChildFromMaster(RecurringJobDefinition master, Instant fireTs) {
    JobEntity child = new JobEntity();
    child.setPayload(master.payload());
    child.setJobType(JobExecutionType.SINGLE);
    child.setStatus(JobStatus.PENDING);
    child.setScheduledTime(fireTs);
    child.setPriority(run.ratchet.api.JobPriority.values()[Math.min(master.priority(), 4)]);
    child.setMaxRetries(master.maxRetries());
    child.setBackoffPolicy(master.backoffPolicy());
    child.setBackoffParamMs(master.backoffParamMs());
    child.setTimeoutSec(master.timeoutSec());
    child.setRecurringMasterId(master.id());
    child.setOnSuccessPayload(master.onSuccessPayload());
    child.setOnFailurePayload(master.onFailurePayload());
    child.setResourceName(master.resourceName());
    child.setIdempotencyKey(UUID.randomUUID().toString());
    return child;
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
