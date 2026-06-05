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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.ri.cdi.ReferenceEncryptionFactory.ReferenceEncryption;

class ReferenceEncryptionFactoryTest {

  private static String key(byte fill) {
    byte[] raw = new byte[32];
    java.util.Arrays.fill(raw, fill);
    return Base64.getEncoder().encodeToString(raw);
  }

  @Test
  void build_noKeys_isEmpty() {
    assertTrue(ReferenceEncryptionFactory.build(null, null, 0L).isEmpty());
    assertTrue(ReferenceEncryptionFactory.build("  ", null, 0L).isEmpty());
  }

  @Test
  void build_singleKey_defaultsCurrentAndUsesAesEngine() {
    Optional<ReferenceEncryption> ref =
        ReferenceEncryptionFactory.build("k1:" + key((byte) 1), null, 0L);

    assertTrue(ref.isPresent());
    assertEquals("AES-256-GCM", ref.get().engine().algorithmId());
    assertEquals("k1", ref.get().keyProvider().currentKey().keyId());
  }

  @Test
  void build_multipleKeys_resolvesNamedCurrent() {
    Optional<ReferenceEncryption> ref =
        ReferenceEncryptionFactory.build(
            "old:" + key((byte) 1) + ",new:" + key((byte) 2), "new", 0L);

    assertTrue(ref.isPresent());
    assertEquals("new", ref.get().keyProvider().currentKey().keyId());
    // Old key stays resolvable for draining rotation.
    assertEquals("old", ref.get().keyProvider().keyById("old").keyId());
  }

  @Test
  void build_multipleKeysWithoutCurrent_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () ->
            ReferenceEncryptionFactory.build(
                "old:" + key((byte) 1) + ",new:" + key((byte) 2), null, 0L));
  }

  @Test
  void build_malformedEntry_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () -> ReferenceEncryptionFactory.build("no-colon-here", null, 0L));
  }

  @Test
  void build_invalidBase64Key_failsLoud() {
    assertThrows(
        EncryptionConfigurationException.class,
        () -> ReferenceEncryptionFactory.build("k1:not-valid-base64!!!", null, 0L));
  }

  @Test
  void nodeEntropy_isStableAndDistinctPerNode() {
    assertEquals(
        ReferenceEncryptionFactory.nodeEntropy("node-a"),
        ReferenceEncryptionFactory.nodeEntropy("node-a"));
    assertNotEquals(
        ReferenceEncryptionFactory.nodeEntropy("node-a"),
        ReferenceEncryptionFactory.nodeEntropy("node-b"));
    assertEquals(0L, ReferenceEncryptionFactory.nodeEntropy(null));
  }
}
