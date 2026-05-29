package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobLogStore;
import run.ratchet.store.spi.LockStore;

@ExtendWith(MockitoExtension.class)
class LogPurgeTimerTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
  private static final String DAILY_CRON = "0 0 2 * * ?";

  @Mock private JobLogStore jobLogStore;
  @Mock private SingletonLeaseService singletonLeaseService;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private LockStore lockStore;

  private LogPurgeTimer timer;

  @BeforeEach
  void setUp() {
    timer = new LogPurgeTimer(jobLogStore, singletonLeaseService, executorProvider, FIXED_CLOCK);
  }

  @Test
  void run_beforeInit_skipsPurgeAndReschedule() {
    assertDoesNotThrow(timer::run);

    verify(singletonLeaseService, never()).tryAcquire(any(), any());
    verify(jobLogStore, never()).purgeLogsOlderThan(any());
    verify(executorProvider, never()).getScheduledExecutor();
    verify(scheduledExecutor, never())
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  void runUsesFixedClockCutoff() {
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    timer.init(7, parsedCron());
    when(singletonLeaseService.tryAcquire(eq("logPurger"), any(Duration.class)))
        .thenReturn(acquiredLease());

    timer.run();

    verify(jobLogStore).purgeLogsOlderThan(FIXED_NOW.minus(Duration.ofDays(7)));
  }

  @Test
  void initSchedulesNextExecutionFromFixedClock() {
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    Cron cron = parsedCron();

    timer.init(7, cron);

    Instant next =
        ExecutionTime.forCron(cron)
            .nextExecution(FIXED_NOW.atZone(ZoneId.systemDefault()))
            .map(ZonedDateTime::toInstant)
            .orElseThrow();
    verify(scheduledExecutor)
        .schedule(
            any(Runnable.class),
            eq(Duration.between(FIXED_NOW, next).toMillis()),
            eq(TimeUnit.MILLISECONDS));
  }

  private static Cron parsedCron() {
    return CRON_PARSER.parse(DAILY_CRON);
  }

  private Optional<SingletonLease> acquiredLease() {
    return Optional.of(new SingletonLease(lockStore, "logPurger", "node-1"));
  }
}
