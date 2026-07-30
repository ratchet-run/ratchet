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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionIntegrity;

/**
 * Verifies the onStartup()/onRuntimeStart() split used to defer encryption resolution on
 * build-time-CDI runtimes (e.g. Quarkus) until {@link RatchetRuntimeStart} fires. The observer now
 * prepares a runtime installation; the common runtime owns the actual holder write.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EncryptionInstallerDeferredStartTest {

  @Mock private Instance<PayloadEncryption> engines;
  @Mock private Instance<KeyProvider> keyProviderInstance;
  @Mock private Instance<MetricsCollector> metricsCollector;
  @Mock private KeyProvider provider;
  @Mock private NodeIdentityProvider nodeIdProvider;

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
    EncryptionIntegrity.clearListener();
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
  }

  @Test
  void onStartup_whenAutoStartDeferred_doesNotInstall() {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");

    installer().onStartup(new Object());

    assertFalse(EncryptionHolder.isEnabled());
  }

  @Test
  void onStartup_whenNotDeferred_preparesRuntimeInstallation() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
    EncryptionInstaller installer = installer();

    installer.onStartup(new Object());

    assertFalse(EncryptionHolder.isEnabled());
    installer.runtimeInstallation().install(installer);
    assertEquals("only", EncryptionHolder.writeEngine().algorithmId());
  }

  @Test
  void onRuntimeStart_preparesInstallation_evenWhileAutoStartIsDeferred() {
    // The realistic Quarkus scenario: the defer flag stays true for the whole process lifetime,
    // and RatchetRuntimeStart is the only thing that ever triggers install().
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");

    EncryptionInstaller installer = installer();
    installer.onRuntimeStart(new RatchetRuntimeStart());

    assertFalse(EncryptionHolder.isEnabled());
    installer.runtimeInstallation().install(installer);
    assertEquals("only", EncryptionHolder.writeEngine().algorithmId());
  }

  private EncryptionInstaller installer() {
    when(engines.stream()).thenReturn(Stream.of(new StubEngine("only")));
    when(keyProviderInstance.isAmbiguous()).thenReturn(false);
    when(keyProviderInstance.isResolvable()).thenReturn(true);
    when(keyProviderInstance.get()).thenReturn(provider);
    when(metricsCollector.isResolvable()).thenReturn(false);
    return new EncryptionInstaller(
        engines, keyProviderInstance, metricsCollector, nodeIdProvider, options());
  }

  private static RatchetOptions options() {
    return RatchetOptions.builder().encryption(e -> e.writeAlgorithm(null)).build();
  }

  private record StubEngine(String algorithmId) implements PayloadEncryption {
    @Override
    public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
      return ciphertext;
    }
  }
}
