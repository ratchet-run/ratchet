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
package run.ratchet.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.inject.qualifiers.Qualifiers;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;

class RatchetEncryptionConfigurationTest {
  @Test
  void multipleEnginesRequireWriteAlgorithm() {
    try (var context = TestContexts.core(Map.of())) {
      var first = mock(PayloadEncryption.class);
      var second = mock(PayloadEncryption.class);
      when(first.algorithmId()).thenReturn("first");
      when(second.algorithmId()).thenReturn("second");
      context.registerSingleton(PayloadEncryption.class, first, Qualifiers.byName("first"));
      context.registerSingleton(PayloadEncryption.class, second, Qualifiers.byName("second"));
      context.registerSingleton(KeyProvider.class, mock(KeyProvider.class));
      Throwable failure = assertThrows(RuntimeException.class, context::start);
      while (failure.getCause() != null) failure = failure.getCause();
      assertInstanceOf(EncryptionConfigurationException.class, failure);
    }
    assertFalse(EncryptionHolder.isEnabled());
  }

  @Test
  void customPairTakesPrecedenceOverInvalidReferenceProperties() {
    var engine = mock(PayloadEncryption.class);
    when(engine.algorithmId()).thenReturn("custom");
    var keys = mock(KeyProvider.class);
    try (var context =
        TestContexts.core(
            Map.of(
                "ratchet.encryption.enabled",
                true,
                "ratchet.encryption.write-algorithm",
                "custom",
                "ratchet.encryption.keys",
                "invalid-unused"))) {
      context.registerSingleton(PayloadEncryption.class, engine);
      context.registerSingleton(KeyProvider.class, keys);
      context.start();
      assertSame(engine, EncryptionHolder.engine("custom"));
      assertSame(keys, EncryptionHolder.keyProvider());
      assertTrue(EncryptionHolder.isGloballyEnabled());
    }
    assertFalse(EncryptionHolder.isEnabled());
  }

  @Test
  void partialCustomStackIsRejected() {
    String key = Base64.getEncoder().encodeToString(new byte[32]);
    try (var context = TestContexts.core(Map.of("ratchet.encryption.keys", "test:" + key))) {
      context.registerSingleton(KeyProvider.class, mock(KeyProvider.class));
      assertThrows(RuntimeException.class, context::start);
    }
    assertFalse(EncryptionHolder.isEnabled());
  }

  @Test
  void environmentReferenceKeysInstallAndReleaseOnClose() {
    String key = Base64.getEncoder().encodeToString(new byte[32]);
    try (var context =
        TestContexts.engine(
            Map.of("ratchet.encryption.enabled", true, "ratchet.encryption.keys", "test:" + key))) {
      context.start();
      assertTrue(EncryptionHolder.isGloballyEnabled());
      assertTrue(EncryptionHolder.encryptionActiveFor(false));
    }
    assertFalse(EncryptionHolder.isEnabled());
  }
}
