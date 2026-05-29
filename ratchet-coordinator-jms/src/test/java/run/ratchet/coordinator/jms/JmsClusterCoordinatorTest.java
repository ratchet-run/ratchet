package run.ratchet.coordinator.jms;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Priority;
import jakarta.interceptor.Interceptor;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.NodeIdentity;
import run.ratchet.api.SignalDecision;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;

class JmsClusterCoordinatorTest {

  private NodeIdentityProvider identityProvider;
  private JmsCoordinatorConfig config;
  private JmsConnectionLifecycle lifecycle;
  private Topic topic;
  private RecordingMetrics metrics;
  private NotifyPayloadCodec codec;

  @BeforeEach
  void setUp() throws Exception {
    identityProvider = () -> "nodeA";
    config = newConfig();
    lifecycle = mock(JmsConnectionLifecycle.class);
    topic = mock(Topic.class);
    metrics = new RecordingMetrics();
    codec = new NotifyPayloadCodec();
    when(lifecycle.sendTextMessage(anyString(), anyString(), anyString())).thenReturn(true);
  }

  @Test
  void closeIsIdempotent() {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    c.close();
    assertDoesNotThrow(c::close);
    // Lifecycle close should fire only once even though SPI close is called twice.
    verify(lifecycle, times(1)).close();
  }

  @Test
  void notifyNewWorkSendsTextMessageWithProperties() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"));

    verify(lifecycle).sendTextMessage(anyString(), eq("nodeA"), eq("HIGH"));
    assertEquals(1, metrics.published("success"));
  }

  @Test
  void notifyNewWorkSwallowsJmsRuntimeException() throws Exception {
    when(lifecycle.sendTextMessage(anyString(), anyString(), anyString()))
        .thenThrow(new JMSRuntimeException("send blew up"));
    JmsClusterCoordinator c = newCoordinator();
    c.init();

    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA")));
    assertEquals(1, metrics.published("failure"));
    verify(lifecycle).triggerReconnect();
  }

  @Test
  void notifyNewWorkSwallowsRuntimeExceptionFromCodec() throws Exception {
    doThrow(new RuntimeException("encode blew up"))
        .when(lifecycle)
        .sendTextMessage(anyString(), anyString(), anyString());
    JmsClusterCoordinator c = newCoordinator();
    c.init();

    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA")));
    assertEquals(1, metrics.published("failure"));
  }

  @Test
  void notifyNewWorkOnUnavailableLifecycleDegradesToNoOp() throws Exception {
    when(lifecycle.sendTextMessage(anyString(), anyString(), anyString())).thenReturn(false);
    JmsClusterCoordinator c = newCoordinator();
    c.init();

    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA")));
    assertEquals(1, metrics.published("failure"));
  }

  @Test
  void notifyNewWorkAfterCloseIsNoOp() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    c.close();
    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA")));
    verify(lifecycle, never()).sendTextMessage(anyString(), anyString(), anyString());
  }

  @Test
  void onJmsMessageSelfSuppressesLocalEnvelope() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<NodeIdentity> received = new CopyOnWriteArrayList<>();
    c.registerWakeupListener((p, src) -> received.add(src));

    TextMessage selfMsg = mock(TextMessage.class);
    when(selfMsg.getText())
        .thenReturn(
            codec.encode(NotifyPayload.current(new NodeIdentity("nodeA"), JobPriority.HIGH)));
    c.onJmsMessage(selfMsg);

    Thread.sleep(100);
    assertEquals(0, received.size(), "self envelope must not fire listener");
    assertEquals(1, metrics.received("ignored_self"));
  }

  @Test
  void onJmsMessageDispatchesRemoteEnvelopeToListener() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<NodeIdentity> received = new CopyOnWriteArrayList<>();
    c.registerWakeupListener((p, src) -> received.add(src));

    TextMessage remote = mock(TextMessage.class);
    when(remote.getText())
        .thenReturn(
            codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.NORMAL)));
    c.onJmsMessage(remote);

    Thread.sleep(100);
    assertEquals(1, received.size());
    assertEquals(new NodeIdentity("nodeB"), received.get(0));
    assertEquals(1, metrics.received("delivered"));
  }

  @Test
  void onJmsMessageSwallowsNonTextMessage() {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    c.onJmsMessage(mock(jakarta.jms.Message.class));
    assertEquals(1, metrics.received("parse_failure"));
  }

  @Test
  void onJmsMessageRejectsOversizedPayload() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();

    TextMessage tm = mock(TextMessage.class);
    when(tm.getText()).thenReturn("x".repeat(17_000)); // > default 16_384 cap

    c.onJmsMessage(tm);

    assertEquals(1, metrics.received("parse_failure"));
  }

  @Test
  void onJmsMessageSwallowsDecodeException() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    TextMessage garbage = mock(TextMessage.class);
    when(garbage.getText()).thenReturn("not json");
    c.onJmsMessage(garbage);
    assertEquals(1, metrics.received("parse_failure"));
  }

  @Test
  void onJmsMessageBuffersWhenNoListenerRegistered() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    TextMessage remote = mock(TextMessage.class);
    when(remote.getText())
        .thenReturn(
            codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOW)));
    c.onJmsMessage(remote);
    assertEquals(1, metrics.received("delivered"));

    // Now register and the buffered message must drain.
    CopyOnWriteArrayList<JobPriority> got = new CopyOnWriteArrayList<>();
    c.registerWakeupListener((p, src) -> got.add(p));
    Thread.sleep(100);
    assertEquals(1, got.size());
    assertEquals(JobPriority.LOW, got.get(0));
  }

  @Test
  void multipleListenersAllReceive() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<String> got1 = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<String> got2 = new CopyOnWriteArrayList<>();
    c.registerWakeupListener((p, src) -> got1.add(src.value()));
    c.registerWakeupListener((p, src) -> got2.add(src.value()));

    TextMessage remote = mock(TextMessage.class);
    when(remote.getText())
        .thenReturn(
            codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.HIGH)));
    c.onJmsMessage(remote);

    Thread.sleep(150);
    assertEquals(1, got1.size());
    assertEquals(1, got2.size());
  }

  @Test
  void throwingListenerDoesNotBlockOthers() throws Exception {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<JobPriority> good = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(
        (p, src) -> {
          throw new RuntimeException("boom");
        });
    c.registerWakeupListener((p, src) -> good.add(p));

    TextMessage remote = mock(TextMessage.class);
    when(remote.getText())
        .thenReturn(
            codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOWEST)));
    c.onJmsMessage(remote);

    Thread.sleep(150);
    assertEquals(1, good.size());
    assertEquals(1, metrics.received("listener_failure"));
  }

  @Test
  void initWiresLocalIdentity() {
    JmsClusterCoordinator c = newCoordinator();
    c.init();
    // No public getter, but afterStart should not throw.
    assertDoesNotThrow(c::afterStart);
    verify(lifecycle).start(any(NodeIdentity.class));
  }

  // ---- helpers ------------------------------------------------------------

  private JmsClusterCoordinator newCoordinator() {
    return new JmsClusterCoordinator(identityProvider, config, lifecycle, topic, metrics);
  }

  private static JmsCoordinatorConfig newConfig() {
    return new JmsCoordinatorConfig(
        Optional.empty(), Optional.empty(), Optional.empty(), true, 25L, 100L, 16_384, 2, 1_000L);
  }

  /** MetricsCollector that records published/received outcomes for assertions. */
  static final class RecordingMetrics implements MetricsCollector {
    private final ConcurrentHashMap<String, AtomicLong> published = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> received = new ConcurrentHashMap<>();

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {
      published.computeIfAbsent(outcome, k -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {
      received.computeIfAbsent(outcome, k -> new AtomicLong()).incrementAndGet();
    }

    long published(String outcome) {
      AtomicLong c = published.get(outcome);
      return c == null ? 0L : c.get();
    }

    long received(String outcome) {
      AtomicLong c = received.get(outcome);
      return c == null ? 0L : c.get();
    }

    // unused MetricsCollector surface
    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(UUID jobId, JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}

    @Override
    public void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void signalWaiting(UUID jobId, JobType type, String signalKey) {}

    @Override
    public void signalDelivered(UUID j, JobType t, String k, SignalDecision.Outcome o) {}

    @Override
    public void signalTimedOut(UUID jobId, JobType type, String signalKey) {}

    @Override
    public void signalCancelled(UUID jobId, JobType type, String signalKey) {}

    @Override
    public void storeOperation(String s, String op, String oc, long n) {}

    @Override
    public void pollerBreakerState(String breakerName, String state) {}
  }

  @Test
  void priorityIsPlatformBeforePlus300() {
    Priority p = JmsClusterCoordinator.class.getAnnotation(Priority.class);
    assertEquals(Interceptor.Priority.PLATFORM_BEFORE + 300, p.value());
  }
}
