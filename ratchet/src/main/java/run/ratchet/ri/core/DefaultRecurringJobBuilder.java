package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.TagStore;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * Default implementation of {@link RecurringJobBuilder} for the ratchet reference implementation.
 *
 * <p>Creates a RECURRING type job with a cron expression and timezone. The {@link
 * RecurringScheduler} will spawn SINGLE child jobs at each scheduled execution time.
 */
@Transactional
public class DefaultRecurringJobBuilder implements RecurringJobBuilder {

  private static final Logger log = Logger.getLogger(DefaultRecurringJobBuilder.class);

  private final String cronExpr;
  private final ZoneId zone;
  private final SerializableCheckedRunnable task;
  private final JobCrudStore jobCrudStore;
  private final TagStore tagStore;
  private final RecurringScheduler recurringScheduler;

  private JobOptions options = JobOptions.defaults();
  private List<String> tags = new ArrayList<>();
  private String businessKey;

  DefaultRecurringJobBuilder(
      String cronExpr,
      ZoneId zone,
      SerializableCheckedRunnable task,
      JobCrudStore jobCrudStore,
      TagStore tagStore,
      RecurringScheduler recurringScheduler) {
    this.cronExpr = cronExpr;
    this.zone = zone;
    this.task = task;
    this.jobCrudStore = jobCrudStore;
    this.tagStore = tagStore;
    this.recurringScheduler = recurringScheduler;
  }

  @Override
  public RecurringJobBuilder withOptions(JobOptions options) {
    this.options = options;
    return this;
  }

  @Override
  public RecurringJobBuilder withTags(List<String> tags) {
    this.tags = tags != null ? tags : new ArrayList<>();
    return this;
  }

  @Override
  public RecurringJobBuilder withBusinessKey(String key) {
    this.businessKey = (key != null && !key.isBlank()) ? key.trim() : null;
    return this;
  }

  @Override
  public JobHandle submit() {
    // Validate and parse cron expression
    Cron cron = RecurringScheduler.PARSER.parse(cronExpr);
    cron.validate();

    // Calculate next fire time
    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    ZonedDateTime now = ZonedDateTime.now(zone);
    Instant nextFire =
        executionTime
            .nextExecution(now)
            .map(ZonedDateTime::toInstant)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Cron expression '" + cronExpr + "' has no future execution time"));

    // Create the recurring job entity
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.RECURRING);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(options.priority());
    job.setScheduledTime(Instant.now());
    job.setPayload(JobPayloadFactory.fromLambda(task));
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setBusinessKey(businessKey);
    job.setCronExpr(cronExpr);
    job.setZoneId(zone.getId());
    job.setNextFire(nextFire);
    job.setMaxRetries(options.maxRetries());
    job.setBackoffPolicy(options.backoffPolicy());
    job.setBackoffParamMs((int) options.backoffParam().toMillis());
    job.setTimeoutSec(options.timeoutSec());

    JobEntity saved = jobCrudStore.save(job);

    // Insert tags if present
    if (!tags.isEmpty()) {
      tagStore.insertTags(saved.getId(), tags);
    }

    log.infof(
        "Recurring job submitted (id=%s, cron=%s, zone=%s, nextFire=%s)",
        saved.getId(), cronExpr, zone, nextFire);

    // Notify the recurring scheduler to check for new jobs immediately
    recurringScheduler.kick();

    return saved::getId;
  }
}
