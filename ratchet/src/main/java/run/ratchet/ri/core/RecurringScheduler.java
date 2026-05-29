package run.ratchet.ri.core;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;

/**
 * SPI for the recurring-job scheduler. Default implementation: {@link
 * run.ratchet.ri.core.internal.DefaultRecurringScheduler}.
 *
 * @apiNote Framework SPI consumed by ri.cdi.RatchetLifecycle, ri.core.DefaultJobSchedulerService /
 *     DefaultJobCreationService, and by ratchet-testsuite integration tests. Applications must not
 *     implement this interface.
 */
public interface RecurringScheduler {

  /** Shared cron parser used by recurring-job consumers throughout the RI. */
  CronParser PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  long getCurrentDelayMs();

  void configure(long minPollMs, long maxPollMs, int batchLimit);

  void init();

  /** Forces an immediate poll cycle. */
  void kick();

  void stop();
}
