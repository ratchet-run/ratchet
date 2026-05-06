package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.store.entity.JobLogEntity.LogLevel;

@ExtendWith(MockitoExtension.class)
class JBossLoggingJobLoggerTest {

  @Mock private InternalEventPublisher eventPublisher;

  @Test
  void info_publishesInfoLogLine() {
    UUID jobId = UUID.randomUUID();
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.info("hello info");

    verify(eventPublisher)
        .publish(
            argThat(
                line -> {
                  JobLogLine log = (JobLogLine) line;
                  return log.jobId().equals(jobId)
                      && log.level() == LogLevel.INFO
                      && log.message().equals("hello info")
                      && log.timestamp() != null;
                }));
  }

  @Test
  void warn_publishesWarnLogLine() {
    UUID jobId = UUID.randomUUID();
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.warn("a warning");

    verify(eventPublisher).publish(argThat(line -> ((JobLogLine) line).level() == LogLevel.WARN));
  }

  @Test
  void error_publishesErrorLogLine() {
    UUID jobId = UUID.randomUUID();
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.error("an error");

    verify(eventPublisher).publish(argThat(line -> ((JobLogLine) line).level() == LogLevel.ERROR));
  }

  @Test
  void debug_publishesDebugLogLine() {
    UUID jobId = UUID.randomUUID();
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.debug("debug info");

    verify(eventPublisher).publish(argThat(line -> ((JobLogLine) line).level() == LogLevel.DEBUG));
  }

  @Test
  void trace_publishesTraceLogLine() {
    UUID jobId = UUID.randomUUID();
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.trace("trace message");

    verify(eventPublisher).publish(argThat(line -> ((JobLogLine) line).level() == LogLevel.TRACE));
  }

  @Test
  void publishedLine_carriesCorrectJobId() {
    UUID jobId = new UUID(0L, 42L);
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.info("msg");

    verify(eventPublisher)
        .publish(argThat(line -> ((JobLogLine) line).jobId().equals(new UUID(0L, 42L))));
  }

  @Test
  void nullPublisher_doesNotThrow() {
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(UUID.randomUUID(), null);

    assertDoesNotThrow(() -> logger.info("test"));
    assertDoesNotThrow(() -> logger.warn("test"));
    assertDoesNotThrow(() -> logger.error("test"));
    assertDoesNotThrow(() -> logger.debug("test"));
    assertDoesNotThrow(() -> logger.trace("test"));
  }

  @Test
  void nullPublisher_noPublishAttempted() {
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(UUID.randomUUID(), null);

    logger.info("will not publish");

    verifyNoInteractions(eventPublisher);
  }

  @Test
  void publishedLine_timestampIsSet() {
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(UUID.randomUUID(), eventPublisher);

    logger.info("timestamped");

    verify(eventPublisher).publish(argThat(line -> ((JobLogLine) line).timestamp() != null));
  }

  @Test
  void formatVariant_interpolatesAndPublishes() {
    UUID jobId = UUID.randomUUID();
    JBossLoggingJobLogger logger = new JBossLoggingJobLogger(jobId, eventPublisher);

    logger.info("job {} attempt {}", "x", 3);

    verify(eventPublisher)
        .publish(
            argThat(
                line -> {
                  String msg = ((JobLogLine) line).message();
                  return msg.contains("x") && msg.contains("3");
                }));
  }
}
