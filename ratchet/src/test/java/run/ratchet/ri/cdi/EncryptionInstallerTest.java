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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EncryptionInstallerTest {

  @Mock private Instance<PayloadEncryption> engines;
  @Mock private Instance<KeyProvider> keyProvider;
  @Mock private Instance<MetricsCollector> metricsCollector;
  @Mock private KeyProvider provider;

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
  }

  @Test
  void multipleEngines_withWriteAlgorithm_installTheNamedWriteEngine() {
    installerWith(options("engine-b"), new StubEngine("engine-a"), new StubEngine("engine-b"))
        .onStartup(new Object());

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
    installerWith(options(null), new StubEngine("only")).onStartup(new Object());

    assertEquals("only", EncryptionHolder.writeEngine().algorithmId());
  }

  private EncryptionInstaller installerWith(
      RatchetOptions options, PayloadEncryption... installed) {
    when(engines.stream()).thenReturn(Stream.of(installed));
    when(keyProvider.isAmbiguous()).thenReturn(false);
    when(keyProvider.isResolvable()).thenReturn(true);
    when(keyProvider.get()).thenReturn(provider);
    when(metricsCollector.isResolvable()).thenReturn(false);
    return new EncryptionInstaller(engines, keyProvider, metricsCollector, options);
  }

  private static RatchetOptions options(String writeAlgorithm) {
    return RatchetOptions.builder().encryption(e -> e.writeAlgorithm(writeAlgorithm)).build();
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
