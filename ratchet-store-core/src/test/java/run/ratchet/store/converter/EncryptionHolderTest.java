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
package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;

class EncryptionHolderTest {

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
  }

  @Test
  void default_isDisabled() {
    assertFalse(EncryptionHolder.isEnabled());
    assertThrows(IllegalStateException.class, EncryptionHolder::writeEngine);
    assertThrows(IllegalStateException.class, EncryptionHolder::keyProvider);
  }

  @Test
  void install_enablesAndResolvesWriteEngineAndProvider() {
    PayloadEncryption engine = new StubEngine("alg-1");
    KeyProvider provider = new StubKeyProvider();

    EncryptionHolder.install(List.of(engine), "alg-1", provider, true);

    assertTrue(EncryptionHolder.isEnabled());
    assertSame(engine, EncryptionHolder.writeEngine());
    assertSame(engine, EncryptionHolder.engine("alg-1"));
    assertSame(provider, EncryptionHolder.keyProvider());
  }

  @Test
  void install_multipleEngines_dispatchesReadByAlgorithmId() {
    PayloadEncryption v1 = new StubEngine("alg-1");
    PayloadEncryption v2 = new StubEngine("alg-2");

    EncryptionHolder.install(List.of(v1, v2), "alg-2", new StubKeyProvider(), true);

    // Writes use the designated engine; reads of an older algorithm still resolve.
    assertSame(v2, EncryptionHolder.writeEngine());
    assertSame(v1, EncryptionHolder.engine("alg-1"));
    assertSame(v2, EncryptionHolder.engine("alg-2"));
  }

  @Test
  void engine_unknownAlgorithm_isPoison() {
    EncryptionHolder.install(
        List.of(new StubEngine("alg-1")), "alg-1", new StubKeyProvider(), true);
    assertThrows(PayloadDecryptionException.class, () -> EncryptionHolder.engine("alg-missing"));
  }

  @Test
  void install_emptyEngines_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () -> EncryptionHolder.install(List.of(), "alg-1", new StubKeyProvider(), true));
  }

  @Test
  void install_nullProvider_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () -> EncryptionHolder.install(List.of(new StubEngine("alg-1")), "alg-1", null, true));
  }

  @Test
  void install_duplicateAlgorithmId_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () ->
            EncryptionHolder.install(
                List.of(new StubEngine("dup"), new StubEngine("dup")),
                "dup",
                new StubKeyProvider(),
                true));
  }

  @Test
  void install_blankAlgorithmId_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () ->
            EncryptionHolder.install(
                List.of(new StubEngine("  ")), "  ", new StubKeyProvider(), true));
  }

  @Test
  void install_writeAlgorithmNotInstalled_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () ->
            EncryptionHolder.install(
                List.of(new StubEngine("alg-1")), "alg-other", new StubKeyProvider(), true));
  }

  @Test
  void disable_revertsToDisabled() {
    EncryptionHolder.install(
        List.of(new StubEngine("alg-1")), "alg-1", new StubKeyProvider(), true);
    EncryptionHolder.disable();
    assertFalse(EncryptionHolder.isEnabled());
  }

  @Test
  void encryptionActiveFor_globalOn_encryptsEveryJob() {
    EncryptionHolder.install(
        List.of(new StubEngine("alg-1")), "alg-1", new StubKeyProvider(), true);
    assertTrue(EncryptionHolder.isGloballyEnabled());
    assertTrue(EncryptionHolder.encryptionActiveFor(false));
    assertTrue(EncryptionHolder.encryptionActiveFor(true));
  }

  @Test
  void encryptionActiveFor_globalOff_onlyEncryptsOptedInJobs() {
    EncryptionHolder.install(
        List.of(new StubEngine("alg-1")), "alg-1", new StubKeyProvider(), false);
    assertFalse(EncryptionHolder.isGloballyEnabled());
    assertFalse(EncryptionHolder.encryptionActiveFor(false));
    assertTrue(EncryptionHolder.encryptionActiveFor(true));
  }

  @Test
  void encryptionActiveFor_disabled_falseWhenNotWanted_butFailsLoudWhenWanted() {
    // No engine installed.
    assertFalse(EncryptionHolder.encryptionActiveFor(false));
    assertThrows(
        EncryptionConfigurationException.class, () -> EncryptionHolder.encryptionActiveFor(true));
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

  private static final class StubKeyProvider implements KeyProvider {
    @Override
    public EncryptionKey currentKey() {
      return () -> "key-1";
    }

    @Override
    public EncryptionKey keyById(String keyId) {
      return () -> keyId;
    }
  }
}
