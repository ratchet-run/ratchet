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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * Known-answer test for {@link HChaCha20} against draft-irtf-cfrg-xchacha-03 Appendix A.1. This
 * locks the one hand-rolled primitive to the spec, independent of the engine that consumes it: if
 * this passes, the derivation's word order, endianness, round count, and the no-feed-forward output
 * selection are all correct.
 */
class HChaCha20Test {

  private static final HexFormat HEX = HexFormat.of();

  // draft-irtf-cfrg-xchacha-03 Appendix A.1.
  private static final byte[] KEY =
      HEX.parseHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
  private static final byte[] NONCE = HEX.parseHex("000000090000004a0000000031415927");
  private static final byte[] EXPECTED_SUBKEY =
      HEX.parseHex("82413b4227b27bfed30e42508a877d73a0f9e4d58a74a853c12ec41326d3ecdc");

  @Test
  void subkey_matchesAppendixA1Vector() {
    assertArrayEquals(EXPECTED_SUBKEY, HChaCha20.subkey(KEY, NONCE));
  }

  @Test
  void subkey_rejectsWrongLengths() {
    assertThrows(
        IllegalArgumentException.class, () -> HChaCha20.subkey(new byte[31], new byte[16]));
    assertThrows(
        IllegalArgumentException.class, () -> HChaCha20.subkey(new byte[32], new byte[15]));
  }

  @Test
  void subkey_isDeterministic() {
    assertArrayEquals(HChaCha20.subkey(KEY, NONCE), HChaCha20.subkey(KEY, NONCE));
  }

  @Test
  void subkey_dependsOnEveryKeyBit() {
    // A single A.1 vector cannot catch a derivation that drops or mis-indexes part of the key. Flip
    // each of the 256 key bits in turn and require the subkey to change — every input bit must
    // reach
    // the output.
    byte[] base = HChaCha20.subkey(KEY, NONCE);
    for (int byteIndex = 0; byteIndex < KEY.length; byteIndex++) {
      for (int bit = 0; bit < 8; bit++) {
        byte[] perturbed = KEY.clone();
        perturbed[byteIndex] ^= (byte) (1 << bit);
        assertFalse(
            Arrays.equals(base, HChaCha20.subkey(perturbed, NONCE)),
            "flipping key bit " + bit + " of byte " + byteIndex + " must change the subkey");
      }
    }
  }

  @Test
  void subkey_dependsOnEveryNonceBit() {
    byte[] base = HChaCha20.subkey(KEY, NONCE);
    for (int byteIndex = 0; byteIndex < NONCE.length; byteIndex++) {
      for (int bit = 0; bit < 8; bit++) {
        byte[] perturbed = NONCE.clone();
        perturbed[byteIndex] ^= (byte) (1 << bit);
        assertFalse(
            Arrays.equals(base, HChaCha20.subkey(KEY, perturbed)),
            "flipping nonce bit " + bit + " of byte " + byteIndex + " must change the subkey");
      }
    }
  }

  @Test
  void subkey_isNeitherTheKeyNorAllZeros() {
    byte[] subkey = HChaCha20.subkey(KEY, NONCE);
    assertFalse(Arrays.equals(KEY, subkey), "subkey must not echo the key");
    assertFalse(Arrays.equals(new byte[HChaCha20.SUBKEY_BYTES], subkey), "subkey must not be zero");
  }
}
