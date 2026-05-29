package run.ratchet.coordinator.infinispan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

class InfinispanWakeupListenerTest {

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();

  @Test
  void postCommitEventDispatchesDecodedPayload() {
    AtomicInteger dispatched = new AtomicInteger();
    InfinispanWakeupListener listener =
        new InfinispanWakeupListener(codec, 16_384, p -> dispatched.incrementAndGet(), () -> {});

    listener.onEntryCreated(eventWithValue(codec.encode(payload("nodeA", JobPriority.HIGH))));

    assertEquals(1, dispatched.get());
  }

  @Test
  void preCommitEventIsIgnored() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    InfinispanWakeupListener listener =
        new InfinispanWakeupListener(
            codec, 16_384, p -> dispatched.incrementAndGet(), parseFailures::incrementAndGet);

    @SuppressWarnings("unchecked")
    CacheEntryCreatedEvent<String, String> event = mock(CacheEntryCreatedEvent.class);
    when(event.isPre()).thenReturn(true);
    listener.onEntryCreated(event);

    assertEquals(0, dispatched.get(), "pre-commit event must not dispatch");
    assertEquals(0, parseFailures.get(), "pre-commit event must not parse-fail");
  }

  @Test
  void nullValueDuringEvictionRecordsParseFailureWithoutNpe() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    InfinispanWakeupListener listener =
        new InfinispanWakeupListener(
            codec, 16_384, p -> dispatched.incrementAndGet(), parseFailures::incrementAndGet);

    listener.onEntryCreated(eventWithValue(null));

    assertEquals(0, dispatched.get(), "null value must not dispatch");
    assertEquals(1, parseFailures.get(), "null value must increment parse_failure");
  }

  @Test
  void malformedPayloadIncrementsParseFailure() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    InfinispanWakeupListener listener =
        new InfinispanWakeupListener(
            codec, 16_384, p -> dispatched.incrementAndGet(), parseFailures::incrementAndGet);

    listener.onEntryCreated(eventWithValue("not json"));

    assertEquals(0, dispatched.get());
    assertEquals(1, parseFailures.get());
  }

  @Test
  void unsupportedVersionIncrementsParseFailure() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    InfinispanWakeupListener listener =
        new InfinispanWakeupListener(
            codec, 16_384, p -> dispatched.incrementAndGet(), parseFailures::incrementAndGet);

    listener.onEntryCreated(eventWithValue("{\"v\":99,\"node\":\"x\",\"prio\":\"HIGH\"}"));

    assertEquals(0, dispatched.get());
    assertEquals(1, parseFailures.get());
  }

  @Test
  void oversizedPayloadIncrementsParseFailureBeforeDecode() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    InfinispanWakeupListener listener =
        new InfinispanWakeupListener(
            codec, 4, p -> dispatched.incrementAndGet(), parseFailures::incrementAndGet);

    listener.onEntryCreated(eventWithValue("{\"v\":1,\"node\":\"nodeA\",\"prio\":\"HIGH\"}"));

    assertEquals(0, dispatched.get());
    assertEquals(1, parseFailures.get());
  }

  @SuppressWarnings("unchecked")
  private static CacheEntryCreatedEvent<String, String> eventWithValue(String value) {
    CacheEntryCreatedEvent<String, String> event = mock(CacheEntryCreatedEvent.class);
    when(event.isPre()).thenReturn(false);
    when(event.getValue()).thenReturn(value);
    return event;
  }

  private static NotifyPayload payload(String node, JobPriority p) {
    return NotifyPayload.current(new NodeIdentity(node), p);
  }
}
