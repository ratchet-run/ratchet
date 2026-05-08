package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.JobLoggerContext;
import run.ratchet.spi.JobLoggerFactory;

/** Default per-job logger factory backed by JBoss Logging and Ratchet log events. */
@ApplicationScoped
public class DefaultJobLoggerFactory implements JobLoggerFactory {

  private final InternalEventPublisher eventPublisher;

  protected DefaultJobLoggerFactory() {
    this.eventPublisher = null;
  }

  @Inject
  public DefaultJobLoggerFactory(InternalEventPublisher eventPublisher) {
    this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
  }

  @Override
  public JobLogger create(JobLoggerContext context) {
    if (eventPublisher == null) {
      throw new IllegalStateException(
          "DefaultJobLoggerFactory requires an injected InternalEventPublisher");
    }
    return new JBossLoggingJobLogger(context.jobId(), eventPublisher);
  }
}
