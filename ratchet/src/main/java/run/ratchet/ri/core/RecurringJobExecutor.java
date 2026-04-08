package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
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

/**
 * Handles the processing of recurring job instances.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Claiming due recurring jobs from the database
 *   <li>Spawning child jobs for each scheduled execution
 *   <li>Calculating next fire times from cron expressions
 *   <li>Handling missed executions with configurable catch-up limits
 * </ul>
 *
 * @see RecurringScheduler for the scheduling lifecycle manager
 */
@ApplicationScoped
@Transactional
public class RecurringJobExecutor {

  private static final Logger log = Logger.getLogger(RecurringJobExecutor.class);

  /**
   * Maximum number of missed executions to catch up on per recurring job. Prevents thundering herd
   * when a system has been down for a long period.
   */
  private static final int MAX_CATCHUP_COUNT = 10;

  /** Store for job entity CRUD operations. */
  private final JobCrudStore jobCrudStore;

  /** Store for job claiming operations on recurring masters. */
  private final JobClaimStore jobClaimStore;

  /** Tracks the local set of registered annotation keys for the startup grace gate. */
  private final RecurringRegistrationState registrationState;

  // Required by CDI proxy
  protected RecurringJobExecutor() {
    this.jobCrudStore = null;
    this.jobClaimStore = null;
    this.registrationState = null;
  }

  @Inject
  public RecurringJobExecutor(
      JobCrudStore jobCrudStore,
      JobClaimStore jobClaimStore,
      RecurringRegistrationState registrationState) {
    this.jobCrudStore = jobCrudStore;
    this.jobClaimStore = jobClaimStore;
    this.registrationState = registrationState;
  }

  /**
   * Creates and enqueues a child job instance from a recurring master template. The child inherits
   * all execution properties from the master but is scheduled as a one-time SINGLE job for the
   * specific fire time.
   *
   * @param master the recurring job template containing payload and configuration
   * @param fireTs the scheduled execution time for this instance
   */
  void enqueueChild(JobEntity master, Instant fireTs) {
    JobEntity child = createChildFromMaster(master, fireTs);
    jobCrudStore.save(child);
  }

  /**
   * Core processing logic that claims due recurring jobs and spawns their instances.
   *
   * @param batchLimit maximum number of recurring masters to process in this cycle
   * @param nodeId the node identifier for claiming jobs
   * @return the number of recurring jobs processed
   */
  int process(int batchLimit, String nodeId) {
    List<JobEntity> masters = jobClaimStore.claimDueRecurring(batchLimit, nodeId);
    Instant now = Instant.now();
    int firedCount = 0;
    for (JobEntity master : masters) {
      // Startup grace gate: during the first ratchet.recurring.startup-grace-seconds after this
      // node finished its @Recurring registration pass, refuse to fire any master whose business
      // key is not in the local known-keys set. This closes the rolling-deploy race where Node
      // B's newer JAR removes an annotation but Node A (or even Node B itself, before cleanup
      // runs) might claim and fire the orphaned master between scan cycles. We release the
      // claim by clearing pickedBy/pickedAt and saving without advancing nextFire — the master
      // stays PENDING and is eligible for another claim attempt next cycle, by which point the
      // cleanup pass will have removed it (if it really is orphaned) or it will pass the gate
      // (if registration completed and it's known).
      if (registrationState != null && !registrationState.shouldFire(master.getBusinessKey())) {
        log.debugf(
            "Recurring master %s (businessKey=%s) skipped — within startup grace and key not"
                + " in local known set",
            master.getId(), master.getBusinessKey());
        master.setPickedBy(null);
        master.setPickedAt(null);
        jobCrudStore.save(master);
        continue;
      }
      firedCount++;
      Cron cron = RecurringScheduler.PARSER.parse(master.getCronExpr());
      ZoneId zone = ZoneId.of(master.getZoneId());
      ExecutionTime execTime = ExecutionTime.forCron(cron);

      Instant baseTime = master.getNextFire() != null ? master.getNextFire() : now;

      // Enqueue child for the current scheduled fire time
      enqueueChild(master, baseTime.isBefore(now) ? baseTime : now);

      // Catch up on missed executions
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

      // Skip ahead to the next valid future time if still in the past
      while (nextOpt.isPresent() && nextOpt.get().isBefore(now)) {
        nextOpt = execTime.nextExecution(nextOpt.get().atZone(zone)).map(ZonedDateTime::toInstant);
      }

      if (nextOpt.isPresent()) {
        master.setNextFire(nextOpt.get());
        master.setStatus(JobStatus.PENDING);
      } else {
        master.setStatus(JobStatus.CANCELED);
      }
      master.setPickedBy(null);
      master.setPickedAt(null);
      jobCrudStore.save(master);
      log.infof("Recurring job %s fired; next=%s", master.getId(), master.getNextFire());
    }
    return firedCount;
  }

  /**
   * Creates a child job entity from a recurring master template.
   *
   * @param master the recurring master job
   * @param fireTs the scheduled fire time for the child
   * @return a new child job entity
   */
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
