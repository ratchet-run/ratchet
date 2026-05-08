package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.JobLoggerContext;

@ExtendWith(MockitoExtension.class)
class DefaultJobLoggerFactoryTest {

  @Mock private InternalEventPublisher eventPublisher;

  @Test
  void create_fromInjectedFactoryCreatesJbossLogger() {
    DefaultJobLoggerFactory factory = new DefaultJobLoggerFactory(eventPublisher);

    assertInstanceOf(JBossLoggingJobLogger.class, factory.create(context()));
  }

  @Test
  void create_fromNoArgFactoryFailsClearly() {
    DefaultJobLoggerFactory factory = new DefaultJobLoggerFactory();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> factory.create(context()));
    assertTrue(thrown.getMessage().contains("InternalEventPublisher"));
  }

  private static JobLoggerContext context() {
    return new JobLoggerContext(
        UUID.randomUUID(), JobType.SINGLE, JobPriority.NORMAL, "node-1", "alice", Map.of());
  }
}
