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
package run.ratchet.coordinator.hazelcast;

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.api.RatchetOptions;
import run.ratchet.coordinator.common.AbstractPushCoordinator;
import run.ratchet.coordinator.common.CoordinatorSupport;
import run.ratchet.coordinator.common.CoordinatorThreading;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Hazelcast {@link ITopic}-based {@link ClusterCoordinator}: publishes wakeup envelopes as strings
 * on a cluster-wide topic and dispatches inbound messages to registered listeners.
 *
 * <p>Adding this module to a Payara (or any Hazelcast-equipped) deployment flips push-based wakeups
 * on with no application-side beans.xml edits. Activation is via {@link Alternative} + {@link
 * Priority}.
 *
 * <p>Publishes use {@link ITopic#publishAsync} so a slow broker round-trip under Hazelcast client
 * mode does not block the calling thread; the returned {@link CompletionStage} is observed for
 * failures and metric-logged.
 *
 * <p>Self-suppression is receive-side — Hazelcast has no built-in source-node filter on topics.
 * {@link #close()} removes the message-listener registration and shuts down the dispatch executor;
 * the {@link HazelcastInstance} is provider-owned and never stopped here.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 200)
// Coordinator @Priority order: see PostgresqlListenNotifyCoordinator. Operators MUST pull in
// exactly one coordinator module; distinct priorities only prevent CDI ambiguity errors on a
// transitive double-pull.
public class HazelcastClusterCoordinator extends AbstractPushCoordinator
    implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(HazelcastClusterCoordinator.class);

  static final String COORDINATOR_KIND = "hazelcast";

  @Inject NodeIdentityProvider identityProvider;

  /**
   * Resolved lazily in {@link #init()}. The config record has a {@code defaults()} factory but is
   * not a managed bean, so it is injected as an {@link Instance} with a defaults() fallback; a
   * direct {@code @Inject HazelcastCoordinatorConfig} would be an unsatisfied dependency that fails
   * deployment validation out of the box.
   */
  @Inject Instance<HazelcastCoordinatorConfig> configInstance;

  @Inject Instance<RatchetOptions> optionsInstance;

  @Inject @Any Instance<HazelcastInstanceProvider> providerInstance;
  @Inject MetricsCollector metrics;

  private HazelcastCoordinatorConfig config;

  private HazelcastInstance directInstance;
  private ITopic<String> topic;
  private UUID listenerRegistrationId;
  private CoordinatorThreading threading;

  HazelcastClusterCoordinator() {
    // CDI proxy constructor — package-private to keep it out of the public API surface.
  }

  /** Test/non-CDI constructor with a directly-supplied {@link HazelcastInstance}. */
  HazelcastClusterCoordinator(
      NodeIdentityProvider identityProvider,
      HazelcastCoordinatorConfig config,
      HazelcastInstance instance,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.directInstance = Objects.requireNonNull(instance, "instance");
    this.metrics = metrics;
    this.threading = CoordinatorThreading.standalone("ratchet-coordinator-hazelcast");
  }

  @PostConstruct
  void init() {
    if (config == null) {
      config =
          CoordinatorSupport.resolveConfigOrDefault(
              configInstance, HazelcastCoordinatorConfig::defaults);
    }
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    if (threading == null) {
      // CDI/production path: route the dispatch pool through the container's managed thread
      // factory. Standalone is an explicit opt-in via the test constructor.
      RatchetOptions options = CoordinatorSupport.resolveOptionsOrDefault(optionsInstance);
      threading =
          CoordinatorThreading.managed(
              "ratchet-coordinator-hazelcast", options.execution().coordinatorThreadFactoryJndi());
    }
    configureDispatch(
        COORDINATOR_KIND,
        "Hazelcast",
        metrics,
        identityProvider,
        config.maxInboundPayloadChars(),
        threading.newDispatchPool(
            "dispatch", config.listenerExecutorThreads(), config.listenerExecutorQueueCapacity()),
        config.shutdownGraceMs());
    HazelcastInstance hz;
    if (directInstance != null) {
      hz = directInstance;
    } else {
      HazelcastInstanceProvider provider =
          CoordinatorSupport.resolveRequired(
              providerInstance,
              "No HazelcastInstanceProvider available. Provide a @Produces"
                  + " HazelcastInstanceProvider or use the Payara JNDI-bound default.",
              "Multiple HazelcastInstanceProvider beans visible; first match wins. Use"
                  + " @Alternative + @Priority for disambiguation.");
      hz = provider.hazelcastInstance();
    }
    topic = hz.getTopic(config.effectiveTopicName());
  }

  @Override
  public void afterStart() {
    if (isClosed()) {
      return;
    }
    if (topic == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    listenerRegistrationId = topic.addMessageListener(new TopicListener());
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (isClosed()) {
      return;
    }
    try {
      String body = codec.encode(NotifyPayload.current(source, priority, executionTarget));
      CompletionStage<Void> stage = topic.publishAsync(body);
      stage.whenCompleteAsync(
          (v, throwable) -> {
            if (throwable == null) {
              clusterWakeupPublished("success");
            } else {
              clusterWakeupPublished("failure");
              log.warnf(
                  "Hazelcast coordinator async publish failed: %s — wakeup dropped",
                  throwable.getMessage());
            }
          },
          listenerExecutor());
    } catch (RuntimeException ex) {
      clusterWakeupPublished("failure");
      log.warnf(
          ex,
          "Hazelcast coordinator notifyNewWork transport/encode failure: %s — wakeup dropped",
          ex.getMessage());
    }
  }

  /**
   * Hook chain entry point — runs during {@code RatchetLifecycle.onShutdown} after pollers and the
   * execution coordinator have stopped. Delegates to {@link #close()}, which is idempotent.
   */
  @Override
  public void afterStop() {
    close();
  }

  @Override
  public void close() {
    if (!markClosed()) {
      return;
    }
    UUID regId = this.listenerRegistrationId;
    if (regId != null && topic != null) {
      try {
        topic.removeMessageListener(regId);
      } catch (RuntimeException ignored) {
        // best-effort; Hazelcast may already be stopped (provider-driven)
      }
    }
    shutdownListenerExecutor();
  }

  /** Dispatch path from the Hazelcast topic listener thread. */
  void onTopicMessage(String body) {
    NotifyPayload payload;
    try {
      if (rejectIfOversized(body)) {
        return;
      }
      payload = codec.decode(body);
    } catch (RuntimeException parseEx) {
      clusterWakeupReceived("parse_failure");
      log.debugf("Hazelcast coordinator dropped malformed payload: %s", parseEx.getMessage());
      return;
    }
    deliverDecodedPayload(payload);
  }

  /**
   * Hazelcast topic listener — adapts the {@link Message} dispatch path to {@link #onTopicMessage}.
   */
  private final class TopicListener implements MessageListener<String> {
    @Override
    public void onMessage(Message<String> message) {
      try {
        onTopicMessage(message.getMessageObject());
      } catch (RuntimeException ignored) {
        // Already metric-logged inside onTopicMessage; swallow so Hazelcast does not tear the
        // listener registration down on an unhandled exception.
      }
    }
  }
}
