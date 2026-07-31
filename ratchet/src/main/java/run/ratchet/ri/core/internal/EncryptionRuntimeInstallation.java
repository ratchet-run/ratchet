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
package run.ratchet.ri.core.internal;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.ri.core.internal.ReferenceEncryptionFactory.ReferenceEncryption;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionIntegrity;

/**
 * Installs the container-resolved encryption stack into the store-layer runtime holders.
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
public final class EncryptionRuntimeInstallation implements RuntimeInstallation {

  // Keep the existing CDI logger category so extraction does not change operational log routing.
  private static final Logger log = Logger.getLogger("run.ratchet.ri.cdi.EncryptionInstaller");

  private final List<PayloadEncryption> engines;
  private final List<KeyProvider> keyProviders;
  private final RatchetOptions options;
  private final List<MetricsCollector> metricsCollectors;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final EncryptionPlan plan;
  private final EncryptionIntegrity.Listener integrityListener;

  /** Creates an installation from container-neutral collaborator collections. */
  public EncryptionRuntimeInstallation(
      List<PayloadEncryption> engines,
      List<KeyProvider> keyProviders,
      RatchetOptions options,
      List<MetricsCollector> metricsCollectors,
      NodeIdentityProvider nodeIdentityProvider) {
    this.engines = List.copyOf(Objects.requireNonNull(engines, "engines"));
    this.keyProviders = List.copyOf(Objects.requireNonNull(keyProviders, "keyProviders"));
    this.options = Objects.requireNonNull(options, "options");
    this.metricsCollectors =
        List.copyOf(Objects.requireNonNull(metricsCollectors, "metricsCollectors"));
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.plan = resolveEncryptionPlan();
    this.integrityListener = resolveIntegrityMetricsBridge();
  }

  @Override
  public void install(Object ownerToken) {
    EncryptionIntegrity.install(ownerToken, integrityListener);
    try {
      if (plan.disabled()) {
        EncryptionHolder.disable(ownerToken);
      } else {
        EncryptionHolder.install(
            ownerToken,
            plan.engines(),
            plan.writeAlgorithm(),
            plan.keyProvider(),
            plan.globalEnabled());
      }
    } catch (RuntimeException | Error failure) {
      EncryptionIntegrity.uninstall(ownerToken);
      throw failure;
    }
  }

  @Override
  public void uninstall(Object ownerToken) {
    try {
      EncryptionHolder.uninstall(ownerToken);
    } finally {
      EncryptionIntegrity.uninstall(ownerToken);
    }
  }

  private EncryptionPlan resolveEncryptionPlan() {
    boolean globalEnabled = options.encryption() != null && options.encryption().enabled();
    boolean hasEngine = !engines.isEmpty();
    if (keyProviders.size() > 1) {
      throw new EncryptionConfigurationException(
          "Multiple KeyProvider beans are resolvable. Configure exactly one.");
    }
    boolean hasProvider = keyProviders.size() == 1;

    if (!hasEngine && !hasProvider) {
      Optional<ReferenceEncryption> reference =
          ReferenceEncryptionFactory.fromEnvironment(resolveNodeEntropy());
      if (reference.isPresent()) {
        ReferenceEncryption resolved = reference.get();
        return new EncryptionPlan(
            List.of(resolved.engine()),
            resolved.engine().algorithmId(),
            resolved.keyProvider(),
            globalEnabled);
      }
      if (globalEnabled) {
        throw new EncryptionConfigurationException(
            "Payload encryption is enabled (RatchetOptions.encryption) but no PayloadEncryption"
                + " engine and KeyProvider are installed, and no reference keys are configured"
                + " (RATCHET_ENCRYPTION_KEYS).");
      }
      return EncryptionPlan.disabledPlan();
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
    return new EncryptionPlan(engines, resolveWriteAlgorithm(), keyProviders.get(0), globalEnabled);
  }

  private String resolveWriteAlgorithm() {
    String configured = options.encryption() == null ? null : options.encryption().writeAlgorithm();
    if (configured != null && !configured.isBlank()) {
      return configured;
    }
    if (engines.size() == 1) {
      return engines.get(0).algorithmId();
    }
    throw new EncryptionConfigurationException(
        "Multiple PayloadEncryption engines are installed but no write algorithm is configured. Set"
            + " RatchetOptions.encryption().writeAlgorithm to the algorithm id new writes should"
            + " use.");
  }

  private EncryptionIntegrity.Listener resolveIntegrityMetricsBridge() {
    if (metricsCollectors.size() != 1) {
      return null;
    }
    MetricsCollector metrics = metricsCollectors.get(0);
    return (jobId, surface) -> metrics.encryptionIntegrityViolation(jobId, surface.name());
  }

  private long resolveNodeEntropy() {
    if (nodeIdentityProvider == null) {
      warnNoNodeEntropy(null);
      return 0L;
    }
    long entropy;
    try {
      entropy = ReferenceEncryptionFactory.nodeEntropy(nodeIdentityProvider.getNodeId());
    } catch (RuntimeException exception) {
      warnNoNodeEntropy(exception);
      return 0L;
    }
    if (entropy == 0L) {
      warnNoNodeEntropy(null);
    }
    return entropy;
  }

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

  private record EncryptionPlan(
      List<PayloadEncryption> engines,
      String writeAlgorithm,
      KeyProvider keyProvider,
      boolean globalEnabled) {

    private static EncryptionPlan disabledPlan() {
      return new EncryptionPlan(List.of(), null, null, false);
    }

    private boolean disabled() {
      return engines.isEmpty();
    }
  }
}
