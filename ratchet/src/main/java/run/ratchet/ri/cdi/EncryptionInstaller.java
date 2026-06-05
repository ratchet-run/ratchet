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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
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

  private final Instance<PayloadEncryption> engines;
  private final Instance<KeyProvider> keyProvider;
  private final Instance<MetricsCollector> metricsCollector;
  private final RatchetOptions options;

  /**
   * No-arg constructor so Weld can instantiate the client-proxy subclass (CDI 4.0 §3.15); never
   * used for a real instance.
   */
  protected EncryptionInstaller() {
    this.engines = null;
    this.keyProvider = null;
    this.metricsCollector = null;
    this.options = null;
  }

  @Inject
  public EncryptionInstaller(
      Instance<PayloadEncryption> engines,
      Instance<KeyProvider> keyProvider,
      Instance<MetricsCollector> metricsCollector,
      RatchetOptions options) {
    this.engines = engines;
    this.keyProvider = keyProvider;
    this.metricsCollector = metricsCollector;
    this.options = options;
  }

  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object event) {
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
      if (globalEnabled) {
        throw new EncryptionConfigurationException(
            "Payload encryption is enabled (RatchetOptions.encryption) but no PayloadEncryption"
                + " engine and KeyProvider are installed.");
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

  @PreDestroy
  void onShutdown() {
    EncryptionHolder.disable();
    EncryptionIntegrity.clearListener();
  }
}
