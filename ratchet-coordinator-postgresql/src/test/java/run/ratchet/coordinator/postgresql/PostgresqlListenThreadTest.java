package run.ratchet.coordinator.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.postgresql.PGNotification;
import run.ratchet.coordinator.common.NotifyPayloadCodec;

class PostgresqlListenThreadTest {

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();

  @Test
  void oversizedPayloadIncrementsParseFailureBeforeDecode() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    PostgresqlCoordinatorConfig config =
        new PostgresqlCoordinatorConfig(
            "ratchet_wakeup", Optional.empty(), 100L, 10L, 50L, 4, 1, 500L);
    PostgresqlListenThread listener =
        new PostgresqlListenThread(
            new PostgresqlConnectionLifecycle(
                () -> {
                  throw new AssertionError("connection should not be acquired");
                },
                config,
                ms -> {}),
            codec,
            config,
            p -> dispatched.incrementAndGet(),
            parseFailures::incrementAndGet,
            () -> {});
    PGNotification notification = mock(PGNotification.class);
    when(notification.getParameter()).thenReturn("{\"v\":1,\"node\":\"nodeA\",\"prio\":\"HIGH\"}");

    listener.dispatchOne(notification);

    assertEquals(0, dispatched.get());
    assertEquals(1, parseFailures.get());
  }
}
