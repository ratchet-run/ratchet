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
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;

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
 *   <li><b>Enabled</b> — an engine and a provider are both produced. Encryption is installed; the
 *       global switch decides whether every job is encrypted or only opted-in ones.
 *   <li><b>Misconfigured</b> — the global switch is on but nothing is produced, or exactly one of
 *       the engine/provider is produced. Startup aborts with {@link
 *       EncryptionConfigurationException} rather than degrade to plaintext.
 * </ul>
 */
@ApplicationScoped
public class EncryptionInstaller {

  private final Instance<PayloadEncryption> engines;
  private final Instance<KeyProvider> keyProvider;
  private final RatchetOptions options;

  /**
   * No-arg constructor so Weld can instantiate the client-proxy subclass (CDI 4.0 §3.15); never
   * used for a real instance.
   */
  protected EncryptionInstaller() {
    this.engines = null;
    this.keyProvider = null;
    this.options = null;
  }

  @Inject
  public EncryptionInstaller(
      Instance<PayloadEncryption> engines,
      Instance<KeyProvider> keyProvider,
      RatchetOptions options) {
    this.engines = engines;
    this.keyProvider = keyProvider;
    this.options = options;
  }

  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object event) {
    if (engines == null || keyProvider == null || options == null) {
      return;
    }
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
    if (engineList.size() > 1) {
      throw new EncryptionConfigurationException(
          "Multiple PayloadEncryption engines are installed but no active write algorithm is"
              + " configured. This version supports a single write engine; additional engines for"
              + " read-time dispatch arrive with rotation tooling.");
    }
    EncryptionHolder.install(
        engineList, engineList.get(0).algorithmId(), keyProvider.get(), globalEnabled);
  }

  @PreDestroy
  void onShutdown() {
    EncryptionHolder.disable();
  }
}
