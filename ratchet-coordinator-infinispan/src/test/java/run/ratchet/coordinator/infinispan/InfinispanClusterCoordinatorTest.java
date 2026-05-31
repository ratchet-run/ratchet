/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.coordinator.infinispan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Priority;
import jakarta.interceptor.Interceptor;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.infinispan.Cache;
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

class InfinispanClusterCoordinatorTest {

  @SuppressWarnings("unchecked")
  private final Cache<String, String> cache = mock(Cache.class);

  private final NodeIdentityProvider identityProvider = () -> "nodeA";
  private final InfinispanCoordinatorConfig config =
      new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 60L, 16_384, 2, 1_000L);
  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private RecordingMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new RecordingMetrics();
    when(cache.addListenerAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(cache.removeListenerAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(cache.putAsync(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  @Test
  void notifyNewWorkPublishesEncodedEnvelopeWithUniqueKeyAndTtl() {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null);
    c.notifyNewWork(JobPriority.NORMAL, new NodeIdentity("nodeA"), null);

    verify(cache).putAsync(eq("nodeA:1"), anyString(), eq(60L), eq(TimeUnit.SECONDS));
    verify(cache).putAsync(eq("nodeA:2"), anyString(), eq(60L), eq(TimeUnit.SECONDS));
    assertEquals(2, metrics.published("success"));
  }

  @Test
  void notifyNewWorkSwallowsRuntimeExceptions() {
    when(cache.putAsync(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
        .thenThrow(new RuntimeException("boom"));
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();

    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null));
    assertEquals(1, metrics.published("failure"));
  }

  @Test
  void notifyNewWorkAfterCloseIsNoOp() {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    c.afterStart();
    c.close();
    assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null));
    verify(cache, times(0)).putAsync(anyString(), anyString(), anyLong(), any(TimeUnit.class));
  }

  @Test
  void closeIsIdempotent() {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    c.afterStart();
    c.close();
    assertDoesNotThrow(c::close);
    // removeListenerAsync fired exactly once.
    verify(cache, times(1)).removeListenerAsync(any());
  }

  @Test
  void afterStartRegistersListener() {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    c.afterStart();
    verify(cache, atLeastOnce()).addListenerAsync(any(InfinispanWakeupListener.class));
  }

  @Test
  void onInboundNotificationSelfSuppressesLocalEnvelope() throws Exception {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<NodeIdentity> received = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> received.add(hint.source()));

    c.onInboundNotification(NotifyPayload.current(new NodeIdentity("nodeA"), JobPriority.HIGH));

    Thread.sleep(100);
    assertEquals(0, received.size(), "self envelope must not fire listener");
    assertEquals(1, metrics.received("ignored_self"));
  }

  @Test
  void onInboundNotificationDispatchesRemoteEnvelope() throws Exception {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<NodeIdentity> received = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> received.add(hint.source()));

    c.onInboundNotification(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOW));

    Thread.sleep(100);
    assertEquals(1, received.size());
    assertEquals(new NodeIdentity("nodeB"), received.get(0));
    assertEquals(1, metrics.received("delivered"));
  }

  @Test
  void preRegistrationBufferDrainsToLateListener() throws Exception {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();

    c.onInboundNotification(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOW));
    CopyOnWriteArrayList<JobPriority> got = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> got.add(hint.priority()));

    Thread.sleep(100);
    assertEquals(1, got.size());
    assertEquals(JobPriority.LOW, got.get(0));
  }

  @Test
  void multipleListenersAllReceive() throws Exception {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<String> got1 = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<String> got2 = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(hint -> got1.add(hint.source().value()));
    c.registerWakeupListener(hint -> got2.add(hint.source().value()));

    c.onInboundNotification(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.HIGH));

    Thread.sleep(150);
    assertEquals(1, got1.size());
    assertEquals(1, got2.size());
  }

  @Test
  void throwingListenerDoesNotBlockOthers() throws Exception {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    CopyOnWriteArrayList<JobPriority> good = new CopyOnWriteArrayList<>();
    c.registerWakeupListener(
        hint -> {
          throw new RuntimeException("boom");
        });
    c.registerWakeupListener(hint -> good.add(hint.priority()));

    c.onInboundNotification(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.LOWEST));

    Thread.sleep(150);
    assertEquals(1, good.size());
    assertEquals(1, metrics.received("listener_failure"));
  }

  @Test
  void preRegistrationBufferOverflowIncrementsMetric() throws Exception {
    InfinispanClusterCoordinator c = newCoordinator();
    c.init();
    // Buffer capacity = 256; send 300 to exceed it.
    for (int i = 0; i < 300; i++) {
      c.onInboundNotification(NotifyPayload.current(new NodeIdentity("nodeB"), JobPriority.NORMAL));
    }
    Thread.sleep(50);
    // overflow metric incremented at least once (drop-oldest happens on every offer beyond cap).
    assertTrue(metrics.received("pre_registration_overflow") > 0);
  }

  private InfinispanClusterCoordinator newCoordinator() {
    return new InfinispanClusterCoordinator(identityProvider, config, cache, metrics);
  }

  /** Recording MetricsCollector used by these tests. */
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
  void priorityIsPlatformBeforePlus100() {
    Priority p = InfinispanClusterCoordinator.class.getAnnotation(Priority.class);
    assertEquals(Interceptor.Priority.PLATFORM_BEFORE + 100, p.value());
  }
}
