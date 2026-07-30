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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionIntegrity;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EncryptionInstallerTest {

  @Mock private Instance<PayloadEncryption> engines;
  @Mock private Instance<KeyProvider> keyProvider;
  @Mock private Instance<MetricsCollector> metricsCollector;
  @Mock private KeyProvider provider;
  @Mock private NodeIdentityProvider nodeIdProvider;

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
    EncryptionIntegrity.clearListener();
    System.clearProperty(ReferenceEncryptionFactory.KEYS_PROPERTY);
    System.clearProperty(ReferenceEncryptionFactory.CURRENT_KEY_PROPERTY);
  }

  @Test
  void multipleEngines_withWriteAlgorithm_installTheNamedWriteEngine() {
    installFromStartup(
        installerWith(options("engine-b"), new StubEngine("engine-a"), new StubEngine("engine-b")));

    assertEquals("engine-b", EncryptionHolder.writeEngine().algorithmId());
  }

  @Test
  void multipleEngines_withoutWriteAlgorithm_failLoud() {
    EncryptionInstaller installer =
        installerWith(options(null), new StubEngine("engine-a"), new StubEngine("engine-b"));

    assertThrows(EncryptionConfigurationException.class, () -> installer.onStartup(new Object()));
  }

  @Test
  void singleEngine_noWriteAlgorithm_usesTheSoleEngine() {
    installFromStartup(installerWith(options(null), new StubEngine("only")));

    assertEquals("only", EncryptionHolder.writeEngine().algorithmId());
  }

  @Test
  void globalEnabled_nothingInstalled_failLoud() {
    EncryptionInstaller installer = installer(enabledOptions(), false /* providerResolvable */);

    assertThrows(EncryptionConfigurationException.class, () -> installer.onStartup(new Object()));
  }

  @Test
  void disabled_nothingInstalled_staysDisabled() {
    // Seed an installed state so the assertion proves the disable() branch ran, not a stale state.
    EncryptionHolder.install(List.of(new StubEngine("seed")), "seed", provider, false);
    EncryptionInstaller installer = installer(options(null), false /* providerResolvable */);

    installFromStartup(installer);

    assertFalse(EncryptionHolder.isEnabled());
  }

  @Test
  void keyProviderWithoutEngine_failLoud() {
    EncryptionInstaller installer = installer(options(null), true /* providerResolvable */);

    assertThrows(EncryptionConfigurationException.class, () -> installer.onStartup(new Object()));
  }

  @Test
  void engineWithoutKeyProvider_failLoud() {
    EncryptionInstaller installer =
        installer(options(null), false /* providerResolvable */, new StubEngine("only"));

    assertThrows(EncryptionConfigurationException.class, () -> installer.onStartup(new Object()));
  }

  @Test
  void multipleKeyProviders_failLoud() {
    when(keyProvider.isAmbiguous()).thenReturn(true);
    EncryptionInstaller installer =
        installer(options(null), true /* providerResolvable */, new StubEngine("only"));

    assertThrows(EncryptionConfigurationException.class, () -> installer.onStartup(new Object()));
  }

  @Test
  void noAppBeans_referenceKeysConfigured_installsBundledStack() {
    // No application engine/provider, but the deployment supplied reference keys via configuration:
    // the bundled AES-256-GCM engine + SecretKeyProvider are installed instead of failing or
    // silently disabling.
    System.setProperty(ReferenceEncryptionFactory.KEYS_PROPERTY, "k1:" + base64Key());
    System.setProperty(ReferenceEncryptionFactory.CURRENT_KEY_PROPERTY, "k1");
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    EncryptionInstaller installer = installer(enabledOptions(), false /* providerResolvable */);

    installFromStartup(installer);

    assertEquals("AES-256-GCM", EncryptionHolder.writeEngine().algorithmId());
    assertEquals(true, EncryptionHolder.isGloballyEnabled());
  }

  @Test
  void noAppBeans_referenceKeysConfigured_perJobOptInWhenGlobalDisabled() {
    // Keys configured but the global switch off: the stack is still installed (so per-job opt-in
    // works), just not applied to every job.
    System.setProperty(ReferenceEncryptionFactory.KEYS_PROPERTY, "k1:" + base64Key());
    EncryptionInstaller installer = installer(options(null), false /* providerResolvable */);

    installFromStartup(installer);

    assertEquals("AES-256-GCM", EncryptionHolder.writeEngine().algorithmId());
    assertFalse(EncryptionHolder.isGloballyEnabled());
  }

  @Test
  void nodeIdThrows_installsAnyway_andWarnsAboutReducedProtection() {
    System.setProperty(ReferenceEncryptionFactory.KEYS_PROPERTY, "k1:" + base64Key());
    when(nodeIdProvider.getNodeId()).thenThrow(new IllegalStateException("no node id yet"));
    EncryptionInstaller installer = installer(options(null), false /* providerResolvable */);

    CapturingHandler handler = attachHandler();
    try {
      installFromStartup(installer);
    } finally {
      detachHandler(handler);
    }

    // Encryption still installs (no startup abort) but the degraded posture is now visible.
    assertEquals("AES-256-GCM", EncryptionHolder.writeEngine().algorithmId());
    assertTrue(
        handler.warnedAboutEntropy(), "expected a WARN about reduced nonce-clone protection");
  }

  @Test
  void blankNodeId_installsAnyway_andWarnsAboutReducedProtection() {
    System.setProperty(ReferenceEncryptionFactory.KEYS_PROPERTY, "k1:" + base64Key());
    when(nodeIdProvider.getNodeId()).thenReturn("");
    EncryptionInstaller installer = installer(options(null), false /* providerResolvable */);

    CapturingHandler handler = attachHandler();
    try {
      installFromStartup(installer);
    } finally {
      detachHandler(handler);
    }

    assertEquals("AES-256-GCM", EncryptionHolder.writeEngine().algorithmId());
    assertTrue(
        handler.warnedAboutEntropy(), "expected a WARN about reduced nonce-clone protection");
  }

  private static CapturingHandler attachHandler() {
    Logger logger = Logger.getLogger(EncryptionInstaller.class.getName());
    logger.setLevel(Level.ALL);
    CapturingHandler handler = new CapturingHandler();
    logger.addHandler(handler);
    return handler;
  }

  private static void detachHandler(CapturingHandler handler) {
    Logger.getLogger(EncryptionInstaller.class.getName()).removeHandler(handler);
  }

  /** Collects WARN records so a test can assert the degraded-entropy warning was emitted. */
  private static final class CapturingHandler extends Handler {
    private final CopyOnWriteArrayList<LogRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    boolean warnedAboutEntropy() {
      return records.stream()
          .anyMatch(
              r ->
                  r.getLevel().intValue() >= Level.WARNING.intValue()
                      && r.getMessage() != null
                      && r.getMessage().contains("per-node nonce entropy"));
    }
  }

  @Test
  void appBeansTakePrecedence_overReferenceKeys() {
    // An application-provided engine/provider wins even when reference keys are also configured.
    System.setProperty(ReferenceEncryptionFactory.KEYS_PROPERTY, "k1:" + base64Key());
    installFromStartup(installerWith(options(null), new StubEngine("app-engine")));

    assertEquals("app-engine", EncryptionHolder.writeEngine().algorithmId());
  }

  private static String base64Key() {
    return Base64.getEncoder().encodeToString(new byte[32]);
  }

  private static void installFromStartup(EncryptionInstaller installer) {
    installer.onStartup(new Object());
    installer.runtimeInstallation().install(installer);
  }

  /**
   * Builds an installer with caller-controlled engine presence and provider resolvability so the
   * fail-loud branches (no engine, no provider, asymmetric wiring) can be driven independently —
   * unlike {@link #installerWith} which always wires a resolvable provider and an engine.
   */
  private EncryptionInstaller installer(
      RatchetOptions options, boolean providerResolvable, PayloadEncryption... installed) {
    when(engines.stream()).thenReturn(Stream.of(installed));
    when(keyProvider.isResolvable()).thenReturn(providerResolvable);
    return new EncryptionInstaller(engines, keyProvider, metricsCollector, nodeIdProvider, options);
  }

  private EncryptionInstaller installerWith(
      RatchetOptions options, PayloadEncryption... installed) {
    when(engines.stream()).thenReturn(Stream.of(installed));
    when(keyProvider.isAmbiguous()).thenReturn(false);
    when(keyProvider.isResolvable()).thenReturn(true);
    when(keyProvider.get()).thenReturn(provider);
    when(metricsCollector.isResolvable()).thenReturn(false);
    return new EncryptionInstaller(engines, keyProvider, metricsCollector, nodeIdProvider, options);
  }

  private static RatchetOptions options(String writeAlgorithm) {
    return RatchetOptions.builder().encryption(e -> e.writeAlgorithm(writeAlgorithm)).build();
  }

  private static RatchetOptions enabledOptions() {
    return RatchetOptions.builder().encryption(e -> e.enabled(true)).build();
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
