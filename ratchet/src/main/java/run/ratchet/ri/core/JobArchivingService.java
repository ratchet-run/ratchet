package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import java.util.concurrent.Future;

/**
 * SPI for moving completed jobs to archive storage on a retention schedule. Default implementation:
 * {@link run.ratchet.ri.core.internal.DefaultJobArchivingService}.
 *
 * @apiNote Framework SPI consumed by ri.cdi.RatchetLifecycle and by ratchet-testsuite integration
 *     tests. Applications must not implement this interface.
 */
public interface JobArchivingService {

  /** Initializes archive retention settings and schedules the first archive pass when enabled. */
  void init(boolean enabled, long retentionDays, int batchSize, Cron cronExpression);

  /** Submits an archive pass to the job executor without joining the caller's transaction. */
  Future<?> triggerArchiving();

  /** Stops future scheduling for this service. */
  void stop();
}
