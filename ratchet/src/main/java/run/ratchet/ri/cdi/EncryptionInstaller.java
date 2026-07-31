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
import java.util.List;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.ri.core.internal.EncryptionRuntimeInstallation;
import run.ratchet.ri.core.internal.RuntimeInstallation;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;

/**
 * Resolves the framework-managed {@link PayloadEncryption} engines and {@link KeyProvider} into a
 * runtime-owned {@link RuntimeInstallation}.
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
  private volatile RuntimeInstallation runtimeInstallation;
  private volatile Object installedOwnerToken;

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
          @Priority(RatchetRuntimeStart.PRIORITY_ENCRYPTION_INSTALL)
          @Initialized(ApplicationScoped.class) Object event) {
    if (RatchetRuntimeStart.logIfDeferred(
        log,
        "Encryption install deferred pending RatchetRuntimeStart event; if this runtime never"
            + " fires that event, encryption will never install")) {
      return;
    }
    runtimeInstallation();
  }

  void onRuntimeStart(
      @Observes @Priority(RatchetRuntimeStart.PRIORITY_ENCRYPTION_INSTALL)
          RatchetRuntimeStart event) {
    runtimeInstallation();
  }

  public RuntimeInstallation runtimeInstallation() {
    RuntimeInstallation current = runtimeInstallation;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (runtimeInstallation == null) {
        runtimeInstallation = createRuntimeInstallation();
      }
      return runtimeInstallation;
    }
  }

  private RuntimeInstallation createRuntimeInstallation() {
    if (engines == null || keyProvider == null || options == null) {
      return noOpInstallation();
    }
    if (keyProvider.isAmbiguous()) {
      throw new EncryptionConfigurationException(
          "Multiple KeyProvider beans are resolvable. Configure exactly one.");
    }
    List<PayloadEncryption> resolvedEngines = engines.stream().toList();
    if (resolvedEngines.isEmpty() && keyProvider.isResolvable()) {
      throw new EncryptionConfigurationException(
          "A KeyProvider is installed but no PayloadEncryption engine is. Install an engine or"
              + " remove the provider.");
    }
    RuntimeInstallation delegate =
        new EncryptionRuntimeInstallation(
            resolvedEngines,
            keyProvider.isResolvable() ? List.of(keyProvider.get()) : List.of(),
            options,
            metricsCollector != null && metricsCollector.isResolvable()
                ? List.of(metricsCollector.get())
                : List.of(),
            nodeIdProvider);
    return trackingInstallation(delegate);
  }

  private RuntimeInstallation trackingInstallation(RuntimeInstallation delegate) {
    return new RuntimeInstallation() {
      @Override
      public void install(Object ownerToken) {
        delegate.install(ownerToken);
        installedOwnerToken = ownerToken;
      }

      @Override
      public void uninstall(Object ownerToken) {
        delegate.uninstall(ownerToken);
      }
    };
  }

  private static RuntimeInstallation noOpInstallation() {
    return new RuntimeInstallation() {
      @Override
      public void install(Object ownerToken) {}

      @Override
      public void uninstall(Object ownerToken) {}
    };
  }

  @PreDestroy
  void onShutdown() {
    RuntimeInstallation current = runtimeInstallation;
    Object ownerToken = installedOwnerToken;
    if (current != null && ownerToken != null) {
      current.uninstall(ownerToken);
    }
  }
}
