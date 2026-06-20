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
package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.ri.cdi.ReferenceEncryptionFactory.ReferenceEncryption;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionIntegrity;

/**
 * Installs the framework-resolved {@link PayloadEncryption} engines and {@link KeyProvider} into
 * {@link EncryptionHolder} at application startup, and clears it at shutdown so a redeploy does not
 * leak stale state across the static holder.
 *
 * <p>The row mappers and document mapper that apply encryption run outside a CDI container, so they
 * resolve the engine and key provider through the static holder rather than {@code @Inject}. This
 * installer is the bridge.
 *
 * <p><b>Fail loud, never fail open.</b> The startup state is one of three:
 *
 * <ul>
 *   <li><b>Disabled</b> — neither an engine nor a provider is produced and the global switch is
 *       off. The holder stays disabled; an opted-in job later fails loud at write time rather than
 *       silently writing plaintext.
 *   <li><b>Enabled</b> — at least one engine and a provider are produced. Encryption is installed.
 *       New writes use the configured {@code writeAlgorithm}, or the sole engine when only one is
 *       produced; the global switch decides whether every job is encrypted or only opted-in ones.
 *       Installing several engines — old ones kept to decrypt not-yet-drained rows during algorithm
 *       rotation — is supported when {@code writeAlgorithm} names which engine writes.
 *   <li><b>Misconfigured</b> — the global switch is on but nothing is produced, exactly one of the
 *       engine/provider is produced, or several engines are produced without a {@code
 *       writeAlgorithm} to disambiguate writes. Startup aborts with {@link
 *       EncryptionConfigurationException} rather than degrade to plaintext.
 * </ul>
 */
@ApplicationScoped
public class EncryptionInstaller {

  private static final Logger log = Logger.getLogger(EncryptionInstaller.class);

  private final Instance<PayloadEncryption> engines;
  private final Instance<KeyProvider> keyProvider;
  private final Instance<MetricsCollector> metricsCollector;
  private final NodeIdentityProvider nodeIdProvider;
  private final RatchetOptions options;

  /**
   * No-arg constructor so Weld can instantiate the client-proxy subclass (CDI 4.0 §3.15); never
   * used for a real instance.
   */
  protected EncryptionInstaller() {
    this.engines = null;
    this.keyProvider = null;
    this.metricsCollector = null;
    this.nodeIdProvider = null;
    this.options = null;
  }

  @Inject
  public EncryptionInstaller(
      Instance<PayloadEncryption> engines,
      Instance<KeyProvider> keyProvider,
      Instance<MetricsCollector> metricsCollector,
      NodeIdentityProvider nodeIdProvider,
      RatchetOptions options) {
    this.engines = engines;
    this.keyProvider = keyProvider;
    this.metricsCollector = metricsCollector;
    this.nodeIdProvider = nodeIdProvider;
    this.options = options;
  }

  void onStartup(
      @Observes
          @Priority(Interceptor.Priority.APPLICATION + 499)
          @Initialized(ApplicationScoped.class)
          Object event) {
    // Deferred on build-time-CDI runtimes (e.g. Quarkus): resolving node entropy here reaches the
    // node identity provider (DB + executor) at STATIC_INIT, before the persistence unit exists.
    // RatchetRuntimeStart drives it at runtime instead, which also yields a real node id for nonce
    // entropy rather than the degraded fallback.
    if (RatchetRuntimeStart.autoStartDeferred()) {
      return;
    }
    install();
  }

  void onRuntimeStart(
      @Observes @Priority(Interceptor.Priority.APPLICATION + 499) RatchetRuntimeStart event) {
    // Install encryption before DefaultRatchetLifecycle (APPLICATION + 500) starts the poller, so a
    // pending encrypted job is never claimed before the engine is installed.
    install();
  }

  void install() {
    if (engines == null || keyProvider == null || options == null) {
      return;
    }
    registerIntegrityMetricsBridge();
    boolean globalEnabled = options.encryption() != null && options.encryption().enabled();

    List<PayloadEncryption> engineList = engines.stream().toList();
    boolean hasEngine = !engineList.isEmpty();
    if (keyProvider.isAmbiguous()) {
      throw new EncryptionConfigurationException(
          "Multiple KeyProvider beans are resolvable. Configure exactly one.");
    }
    boolean hasProvider = keyProvider.isResolvable();

    if (!hasEngine && !hasProvider) {
      // No application beans. Fall back to the bundled reference stack if the deployment configured
      // keys via the environment; an app that brings its own engine/provider takes precedence and
      // never reaches here.
      Optional<ReferenceEncryption> reference =
          ReferenceEncryptionFactory.fromEnvironment(resolveNodeEntropy());
      if (reference.isPresent()) {
        ReferenceEncryption ref = reference.get();
        EncryptionHolder.install(
            List.of(ref.engine()), ref.engine().algorithmId(), ref.keyProvider(), globalEnabled);
        return;
      }
      if (globalEnabled) {
        throw new EncryptionConfigurationException(
            "Payload encryption is enabled (RatchetOptions.encryption) but no PayloadEncryption"
                + " engine and KeyProvider are installed, and no reference keys are configured"
                + " (RATCHET_ENCRYPTION_KEYS).");
      }
      EncryptionHolder.disable();
      return;
    }
    if (!hasEngine) {
      throw new EncryptionConfigurationException(
          "A KeyProvider is installed but no PayloadEncryption engine is. Install an engine or"
              + " remove the provider.");
    }
    if (!hasProvider) {
      throw new EncryptionConfigurationException(
          "A PayloadEncryption engine is installed but no KeyProvider is. Install a provider or"
              + " remove the engine.");
    }
    EncryptionHolder.install(
        engineList, resolveWriteAlgorithm(engineList), keyProvider.get(), globalEnabled);
  }

  /**
   * Picks the algorithm id new writes use. With a single engine installed it is that engine. With
   * several — the algorithm-rotation case, where an old engine stays installed to decrypt
   * not-yet-drained rows — the deployment must name the write algorithm via {@code
   * RatchetOptions.encryption().writeAlgorithm}; an unset selection is a fail-loud misconfiguration
   * rather than an arbitrary pick. {@link EncryptionHolder#install} validates that the returned id
   * names an installed engine.
   */
  private String resolveWriteAlgorithm(List<PayloadEncryption> engineList) {
    String configured = options.encryption() == null ? null : options.encryption().writeAlgorithm();
    if (configured != null && !configured.isBlank()) {
      return configured;
    }
    if (engineList.size() == 1) {
      return engineList.get(0).algorithmId();
    }
    throw new EncryptionConfigurationException(
        "Multiple PayloadEncryption engines are installed but no write algorithm is configured. Set"
            + " RatchetOptions.encryption().writeAlgorithm to the algorithm id new writes should"
            + " use.");
  }

  /**
   * Bridges the store-layer {@link EncryptionIntegrity} probe — which runs outside CDI in the row
   * mappers — to the container's {@link MetricsCollector}, so a flagged-but-unframed read (ADR Q-D)
   * is both logged (in the probe) and counted as a metric. A no-op when no single MetricsCollector
   * is resolvable.
   */
  private void registerIntegrityMetricsBridge() {
    if (metricsCollector == null || !metricsCollector.isResolvable()) {
      return;
    }
    MetricsCollector metrics = metricsCollector.get();
    EncryptionIntegrity.setListener(
        (jobId, surface) -> metrics.encryptionIntegrityViolation(jobId, surface.name()));
  }

  /**
   * Per-node entropy mixed into the reference engine's nonce epoch so two nodes that share a key
   * produce disjoint nonce spaces. Falls back to {@code 0} when the node id is unavailable.
   */
  private long resolveNodeEntropy() {
    if (nodeIdProvider == null) {
      warnNoNodeEntropy(null);
      return 0L;
    }
    long entropy;
    try {
      entropy = ReferenceEncryptionFactory.nodeEntropy(nodeIdProvider.getNodeId());
    } catch (RuntimeException e) {
      warnNoNodeEntropy(e);
      return 0L;
    }
    if (entropy == 0L) {
      // A blank/empty node id hashes to 0 (ReferenceEncryptionFactory.nodeEntropy), so the engine
      // gets no per-node component even though resolution did not throw.
      warnNoNodeEntropy(null);
    }
    return entropy;
  }

  /**
   * Warns that the reference engine starts with no per-node entropy. Without it, two nodes that
   * share a key and were cloned from the same image draw a less-distinct nonce epoch, weakening the
   * nonce-clone protection. Encryption still works; this is a posture warning, not a startup abort.
   */
  private void warnNoNodeEntropy(RuntimeException cause) {
    if (cause != null) {
      log.warnf(
          cause,
          "Node identity could not be resolved for the reference encryption engine; starting with"
              + " no per-node nonce entropy (reduced nonce-clone protection). Ensure a"
              + " NodeIdentityProvider yields a stable, non-empty node id.");
    } else {
      log.warn(
          "Node identity is unavailable for the reference encryption engine; starting with no"
              + " per-node nonce entropy (reduced nonce-clone protection). Ensure a"
              + " NodeIdentityProvider yields a stable, non-empty node id.");
    }
  }

  @PreDestroy
  void onShutdown() {
    EncryptionHolder.disable();
    EncryptionIntegrity.clearListener();
  }
}
