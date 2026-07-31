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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionIntegrity;

class EncryptionRuntimeInstallationTest {

  @AfterEach
  void resetHolders() {
    EncryptionHolder.disable();
    EncryptionIntegrity.clearListener();
  }

  @Test
  void asymmetricEncryptionWiringAbortsRuntimeConstruction() {
    PayloadEncryption engine = new StubEngine("only");

    assertThrows(
        EncryptionConfigurationException.class,
        () ->
            new EncryptionRuntimeInstallation(
                List.of(engine), List.of(), RatchetOptions.builder().build(), List.of(), null));
  }

  @Test
  void multipleProvidersAbortRuntimeConstruction() {
    PayloadEncryption engine = new StubEngine("only");
    KeyProvider first = org.mockito.Mockito.mock(KeyProvider.class);
    KeyProvider second = org.mockito.Mockito.mock(KeyProvider.class);

    assertThrows(
        EncryptionConfigurationException.class,
        () ->
            new EncryptionRuntimeInstallation(
                List.of(engine),
                List.of(first, second),
                RatchetOptions.builder().build(),
                List.of(),
                null));
  }

  @Test
  void installAndUninstallOwnTheEncryptionHolder() {
    PayloadEncryption engine = new StubEngine("only");
    KeyProvider provider = org.mockito.Mockito.mock(KeyProvider.class);
    EncryptionRuntimeInstallation installation =
        new EncryptionRuntimeInstallation(
            List.of(engine), List.of(provider), RatchetOptions.builder().build(), List.of(), null);
    Object owner = new Object();

    installation.install(owner);

    assertEquals("only", EncryptionHolder.writeEngine().algorithmId());
    installation.uninstall(owner);
    assertFalse(EncryptionHolder.isEnabled());
  }

  private record StubEngine(String algorithmId) implements PayloadEncryption {

    @Override
    public byte[] encrypt(byte[] plaintext, EncryptionContext context) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, EncryptionContext context) {
      return ciphertext;
    }
  }
}
