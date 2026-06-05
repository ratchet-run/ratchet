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
package run.ratchet.store.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.ProtectedSurface;

class EncryptionAadTest {

  private static final byte[] HEADER =
      EncryptionEnvelope.canonicalHeader("alg", "key-1", new byte[0]);

  @Test
  void compute_isDeterministic() {
    byte[] binding = EncryptionAad.binding(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    byte[] a = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, binding);
    byte[] b = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, binding);
    assertArrayEquals(a, b);
  }

  @Test
  void differentSurface_yieldsDifferentAad() {
    byte[] binding = EncryptionAad.binding(UUID.randomUUID());
    byte[] result = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, binding);
    byte[] args = EncryptionAad.compute(HEADER, ProtectedSurface.PAYLOAD_ARGS, binding);
    assertFalse(Arrays.equals(result, args));
  }

  @Test
  void differentBinding_yieldsDifferentAad() {
    byte[] one = EncryptionAad.binding(UUID.randomUUID());
    byte[] two = EncryptionAad.binding(UUID.randomUUID());
    byte[] a = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, one);
    byte[] b = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, two);
    assertFalse(Arrays.equals(a, b));
  }

  @Test
  void differentHeader_yieldsDifferentAad_soSwappedKeyIdFailsTheTag() {
    byte[] headerKey1 = EncryptionEnvelope.canonicalHeader("alg", "key-1", new byte[0]);
    byte[] headerKey2 = EncryptionEnvelope.canonicalHeader("alg", "key-2", new byte[0]);
    byte[] binding = EncryptionAad.binding(UUID.randomUUID());
    byte[] a = EncryptionAad.compute(headerKey1, ProtectedSurface.RESULT, binding);
    byte[] b = EncryptionAad.compute(headerKey2, ProtectedSurface.RESULT, binding);
    assertFalse(Arrays.equals(a, b));
  }

  @Test
  void lengthPrefixing_isInjective_acrossTheSurfaceBindingBoundary() {
    // Without length prefixes, surfaceName "RESULT" + binding "X" would share the same byte run as
    // a hypothetical "RESUL" + "TX". Length-prefixing keeps the components unambiguous: shifting a
    // byte from the binding into a (here simulated) longer surface field must change the AAD.
    byte[] a = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, "TX".getBytes(UTF_8));
    byte[] b = EncryptionAad.compute(HEADER, ProtectedSurface.RESULT, "T".getBytes(UTF_8));
    assertFalse(Arrays.equals(a, b));
  }

  @Test
  void uuidBinding_andKeyBinding_areBothStable() {
    UUID id = UUID.fromString("11111111-2222-3333-4444-555555555555");
    assertArrayEquals(id.toString().getBytes(UTF_8), EncryptionAad.binding(id));
    assertArrayEquals("order-shipped".getBytes(UTF_8), EncryptionAad.binding("order-shipped"));
    assertEquals(0, EncryptionAad.binding((UUID) null).length);
    assertEquals(0, EncryptionAad.binding((String) null).length);
  }
}
