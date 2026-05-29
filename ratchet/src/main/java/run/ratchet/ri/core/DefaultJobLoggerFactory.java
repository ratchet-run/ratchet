package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Objects;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.JobLoggerContext;
import run.ratchet.spi.JobLoggerFactory;

/** Default per-job logger factory backed by JBoss Logging and Ratchet log events. */
@ApplicationScoped
class DefaultJobLoggerFactory implements JobLoggerFactory {

  private final InternalEventPublisher eventPublisher;
  private final Clock clock;

  protected DefaultJobLoggerFactory() {
    this.eventPublisher = null;
    this.clock = null;
  }

  public DefaultJobLoggerFactory(InternalEventPublisher eventPublisher) {
    this(eventPublisher, Clock.systemUTC());
  }

  @Inject
  public DefaultJobLoggerFactory(InternalEventPublisher eventPublisher, Clock clock) {
    this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public JobLogger create(JobLoggerContext context) {
    if (eventPublisher == null) {
      throw new IllegalStateException(
          "DefaultJobLoggerFactory requires an injected InternalEventPublisher");
    }
    return new JBossLoggingJobLogger(context.jobId(), eventPublisher, clock);
  }
}
