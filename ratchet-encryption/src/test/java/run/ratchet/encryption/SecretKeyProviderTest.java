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
package run.ratchet.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;

class SecretKeyProviderTest {

  private static SecretKey key(byte fill) {
    byte[] raw = new byte[32];
    java.util.Arrays.fill(raw, fill);
    return new SecretKeySpec(raw, "AES");
  }

  @Test
  void currentKey_returnsTheDesignatedCurrent() {
    SecretKeyProvider provider = new SecretKeyProvider(Map.of("k1", key((byte) 1)), "k1");

    EncryptionKey current = provider.currentKey();

    assertEquals("k1", current.keyId());
    assertArrayEquals(
        key((byte) 1).getEncoded(), ((LocalEncryptionKey) current).material().getEncoded());
  }

  @Test
  void keyById_resolvesEveryInstalledKey_soRotationDrains() {
    // During rotation the old key stays installed to decrypt not-yet-drained rows while the new key
    // writes.
    Map<String, SecretKey> keys = new LinkedHashMap<>();
    keys.put("old", key((byte) 1));
    keys.put("new", key((byte) 2));
    SecretKeyProvider provider = new SecretKeyProvider(keys, "new");

    assertEquals("new", provider.currentKey().keyId());
    assertEquals("old", provider.keyById("old").keyId());
    assertEquals("new", provider.keyById("new").keyId());
  }

  @Test
  void keyById_unknownId_isPoison() {
    SecretKeyProvider provider = new SecretKeyProvider(Map.of("k1", key((byte) 1)), "k1");

    assertThrows(KeyNotFoundException.class, () -> provider.keyById("retired"));
  }

  @Test
  void fromBase64_decodesAndResolves() {
    String b64 = Base64.getEncoder().encodeToString(key((byte) 7).getEncoded());
    SecretKeyProvider provider = SecretKeyProvider.fromBase64(Map.of("k1", b64), "k1");

    assertArrayEquals(
        key((byte) 7).getEncoded(),
        ((LocalEncryptionKey) provider.currentKey()).material().getEncoded());
  }

  @Test
  void construction_rejectsWrongKeyLength() {
    SecretKey shortKey = new SecretKeySpec(new byte[16], "AES");

    assertThrows(
        EncryptionConfigurationException.class,
        () -> new SecretKeyProvider(Map.of("k1", shortKey), "k1"));
  }

  @Test
  void construction_rejectsCurrentIdNotInstalled() {
    assertThrows(
        EncryptionConfigurationException.class,
        () -> new SecretKeyProvider(Map.of("k1", key((byte) 1)), "missing"));
  }

  @Test
  void construction_rejectsEmptyKeyset() {
    assertThrows(
        EncryptionConfigurationException.class, () -> new SecretKeyProvider(Map.of(), "k1"));
  }

  @Test
  void fromBase64_rejectsInvalidBase64() {
    assertThrows(
        EncryptionConfigurationException.class,
        () -> SecretKeyProvider.fromBase64(Map.of("k1", "not valid base64 !!!"), "k1"));
  }
}
