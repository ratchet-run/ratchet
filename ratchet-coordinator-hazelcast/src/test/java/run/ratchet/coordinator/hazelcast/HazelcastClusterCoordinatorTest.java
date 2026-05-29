package run.ratchet.coordinator.hazelcast;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;
import jakarta.annotation.Priority;
import jakarta.interceptor.Interceptor;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
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

class HazelcastClusterCoordinatorTest {

  @SuppressWarnings("unchecked")
  private final ITopic<String> topic = mock(ITopic.class);

  private final HazelcastInstance instance = mock(HazelcastInstance.class);
  private final NodeIdentityProvider identityProvider = () -> "nodeA";
  private final HazelcastCoordinatorConfig config =
      new HazelcastCoordinatorConfig("ratchet-wakeup", Optional.empty(), 16_384, 2, 1_000L);
  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private RecordingMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new RecordingMetrics();
    when(instance.<String>getTopic(anyString())).thenReturn(topic);
    when(topic.publishAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    when(topic.addMessageListener(any())).thenReturn(UUID.randomUUID());
  }

  @Test
  void notifyNewWorkPublishesEncodedEnvelope() {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null);

    verify(topic).publishAsync(anyString());
    awaitUntil(() -> metrics.published("success") == 1);
  }

  @Test
  void notifyNewWorkSwallowsRuntimeExceptions() {
    when(topic.publishAsync(anyString())).thenThrow(new RuntimeException("boom"));
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();

    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null));
    assertEquals(1, metrics.published("failure"));
  }

  @Test
  void notifyNewWorkRecordsFailureFromAsyncStage() {
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("async boom"));
    when(topic.publishAsync(anyString())).thenReturn(failed);
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();

    c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null);

    assertEquals(
        0,
        metrics.published("success"),
        "async publish must not record success until completion resolves");
    awaitUntil(() -> metrics.published("failure") == 1);
    assertEquals(1, metrics.published("failure"), "async completion recorded failure");
  }

  @Test
  void notifyNewWorkRecordsSuccessOnlyAfterAsyncCompletion() {
    CompletableFuture<Void> pending = new CompletableFuture<>();
    when(topic.publishAsync(anyString())).thenReturn(pending);
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();

    c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null);

    assertEquals(
        0,
        metrics.published("success"),
        "success must not be counted before publishAsync resolves");
    pending.complete(null);
    awaitUntil(() -> metrics.published("success") == 1);
    assertEquals(1, metrics.published("success"), "success recorded on async completion");
    assertEquals(0, metrics.published("failure"));
  }

  @Test
  void notifyNewWorkAfterCloseIsNoOp() {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    c.afterStart();
    c.close();
    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null));
    verify(topic, times(0)).publishAsync(anyString());
  }

  @Test
  void closeIsIdempotent() {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    c.afterStart();
    c.close();
    assertDoesNotThrow(c::close);
    verify(topic, times(1)).removeMessageListener(any(UUID.class));
  }

  @Test
  void afterStartRegistersMessageListener() {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    c.afterStart();
    verify(topic, atLeastOnce()).addMessageListener(any(MessageListener.class));
  }

  @Test
  void onTopicMessageSelfSuppressesLocalEnvelope() throws Exception {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<NodeIdentity> received = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> received.add(hint.source()));

    c.onTopicMessage(
        codec.encode(NotifyPayload.current(new NodeIdentity("nodeA"), JobPriority.HIGH)));

    Thread.sleep(100);
    assertEquals(0, received.size());
    assertEquals(1, metrics.received("ignored_self"));
  }

  @Test
  void onTopicMessageDispatchesRemoteEnvelope() throws Exception {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<NodeIdentity> received = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> received.add(hint.source()));

    c.onTopicMessage(
        codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOW)));

    Thread.sleep(100);
    assertEquals(1, received.size());
    assertEquals(new NodeIdentity("nodeB"), received.get(0));
    assertEquals(1, metrics.received("delivered"));
  }

  @Test
  void onTopicMessageSwallowsDecodeException() {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    c.onTopicMessage("not json");
    assertEquals(1, metrics.received("parse_failure"));
  }

  @Test
  void onTopicMessageRejectsOversizedPayloadBeforeDecode() {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    AtomicLong listenerCalls = new AtomicLong();
    c.registerWakeupListener(hint -> listenerCalls.incrementAndGet());

    c.onTopicMessage("x".repeat(config.maxInboundPayloadChars() + 1));

    assertEquals(1, metrics.received("parse_failure"));
    assertEquals(0, listenerCalls.get());
  }

  @Test
  void preRegistrationBufferDrainsToLateListener() throws Exception {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();

    c.onTopicMessage(
        codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOW)));
    CopyOnWriteArrayList<JobPriority> got = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> got.add(hint.priority()));

    Thread.sleep(100);
    assertEquals(1, got.size());
    assertEquals(JobPriority.LOW, got.get(0));
  }

  @Test
  void multipleListenersAllReceive() throws Exception {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<String> got1 = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<String> got2 = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> got1.add(hint.source().value()));
    c.registerWakeupListener(hint -> got2.add(hint.source().value()));

    c.onTopicMessage(
        codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.HIGH)));

    Thread.sleep(150);
    assertEquals(1, got1.size());
    assertEquals(1, got2.size());
  }

  @Test
  void throwingListenerDoesNotBlockOthers() throws Exception {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<JobPriority> good = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(
        hint -> {
          throw new RuntimeException("boom");
        });
    c.registerWakeupListener(hint -> good.add(hint.priority()));

    c.onTopicMessage(
        codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOWEST)));

    Thread.sleep(150);
    assertEquals(1, good.size());
    assertEquals(1, metrics.received("listener_failure"));
  }

  @Test
  void preRegistrationBufferOverflowIncrementsMetric() throws Exception {
    HazelcastClusterCoordinator c = newCoordinator();
    c.init();
    for (int i = 0; i < 300; i++) {
      c.onTopicMessage(
          codec.encode(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.NORMAL)));
    }
    Thread.sleep(50);
    assertTrue(metrics.received("pre_registration_overflow") > 0);
  }

  private HazelcastClusterCoordinator newCoordinator() {
    return new HazelcastClusterCoordinator(identityProvider, config, instance, metrics);
  }

  private static void awaitUntil(BooleanSupplier condition) {
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

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
  void priorityIsPlatformBeforePlus200() {
    Priority p = HazelcastClusterCoordinator.class.getAnnotation(Priority.class);
    assertEquals(Interceptor.Priority.PLATFORM_BEFORE + 200, p.value());
  }
}
