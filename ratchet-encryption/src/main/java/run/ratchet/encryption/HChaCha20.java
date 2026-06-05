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

/**
 * HChaCha20 key-derivation function (draft-irtf-cfrg-xchacha-03 §2.2): the ChaCha20 permutation
 * applied as a one-way {@code (256-bit key, 128-bit nonce) -> 256-bit subkey} step. It is the
 * single primitive the JDK does not expose, and the only hand-rolled cryptography in the
 * XChaCha20-Poly1305 engine — everything downstream (the ChaCha20 stream, the Poly1305 MAC, the
 * constant-time tag compare) is the audited JDK {@code ChaCha20-Poly1305} cipher keyed by this
 * subkey.
 *
 * <p>XChaCha20 turns the 192-bit XChaCha nonce into a standard IETF ChaCha20-Poly1305 operation:
 * {@code subkey = HChaCha20(key, nonce[0..16])}, then {@code ChaCha20-Poly1305(subkey, 0x00000000 ∥
 * nonce[16..24], ...)}. Because this derivation is the whole reason the engine can use a random
 * 192-bit nonce safely, it is locked by a known-answer test against the spec, not merely exercised
 * for self-consistency. See {@code HChaCha20Test} (Appendix A.1) and the end-to-end Appendix A.3
 * vector in {@code XChaCha20Poly1305KnownAnswerTest}.
 *
 * <p>HChaCha20 differs from the ChaCha20 block function in exactly two ways, and both are the
 * classic places an implementation goes wrong:
 *
 * <ul>
 *   <li><b>No block counter.</b> The full 16 nonce bytes occupy state words 12..15. (In the stream
 *       cipher, word 12 is the counter.)
 *   <li><b>No feed-forward.</b> The initial state is <em>not</em> added back to the permuted state.
 *       The subkey is taken straight from the permuted state.
 * </ul>
 *
 * <p>The 256-bit subkey is state words {@code 0,1,2,3,12,13,14,15} serialized little-endian.
 *
 * <p>Package-private and stateless; the engine calls {@link #subkey(byte[], byte[])} once per
 * operation from arbitrary threads.
 */
final class HChaCha20 {

  /** Required key length in bytes (256-bit). */
  static final int KEY_BYTES = 32;

  /** Required input-nonce length in bytes (the first 128 bits of the XChaCha nonce). */
  static final int NONCE_BYTES = 16;

  /** Produced subkey length in bytes (256-bit). */
  static final int SUBKEY_BYTES = 32;

  /** Number of double-rounds (column + diagonal); 10 double-rounds == 20 rounds. */
  private static final int DOUBLE_ROUNDS = 10;

  /** The ChaCha20 constant "expand 32-byte k" as four little-endian words (sigma). */
  private static final int[] SIGMA = {0x6170_7865, 0x3320_646e, 0x7962_2d32, 0x6b20_6574};

  private HChaCha20() {}

  /**
   * Derives the 256-bit XChaCha20 subkey from a 256-bit key and the first 128 bits of the nonce.
   *
   * @param key a 32-byte key; never {@code null}
   * @param nonce16 the first 16 bytes of the 24-byte XChaCha20 nonce; never {@code null}
   * @return the 32-byte subkey
   */
  static byte[] subkey(byte[] key, byte[] nonce16) {
    if (key.length != KEY_BYTES || nonce16.length != NONCE_BYTES) {
      throw new IllegalArgumentException(
          "HChaCha20 requires a " + KEY_BYTES + "-byte key and a " + NONCE_BYTES + "-byte nonce");
    }

    // Initial state: constants ∥ key (8 words) ∥ nonce (4 words). No block counter — the full nonce
    // fills words 12..15.
    int[] s = new int[16];
    s[0] = SIGMA[0];
    s[1] = SIGMA[1];
    s[2] = SIGMA[2];
    s[3] = SIGMA[3];
    for (int i = 0; i < 8; i++) {
      s[4 + i] = load32LittleEndian(key, i * 4);
    }
    for (int i = 0; i < 4; i++) {
      s[12 + i] = load32LittleEndian(nonce16, i * 4);
    }

    for (int round = 0; round < DOUBLE_ROUNDS; round++) {
      quarterRound(s, 0, 4, 8, 12);
      quarterRound(s, 1, 5, 9, 13);
      quarterRound(s, 2, 6, 10, 14);
      quarterRound(s, 3, 7, 11, 15);
      quarterRound(s, 0, 5, 10, 15);
      quarterRound(s, 1, 6, 11, 12);
      quarterRound(s, 2, 7, 8, 13);
      quarterRound(s, 3, 4, 9, 14);
    }

    // Output the corners (words 0..3 and 12..15) straight from the permuted state — no
    // feed-forward.
    byte[] subkey = new byte[SUBKEY_BYTES];
    store32LittleEndian(s[0], subkey, 0);
    store32LittleEndian(s[1], subkey, 4);
    store32LittleEndian(s[2], subkey, 8);
    store32LittleEndian(s[3], subkey, 12);
    store32LittleEndian(s[12], subkey, 16);
    store32LittleEndian(s[13], subkey, 20);
    store32LittleEndian(s[14], subkey, 24);
    store32LittleEndian(s[15], subkey, 28);
    return subkey;
  }

  /** The ChaCha quarter-round, in place on four state words. */
  private static void quarterRound(int[] s, int a, int b, int c, int d) {
    s[a] += s[b];
    s[d] = Integer.rotateLeft(s[d] ^ s[a], 16);
    s[c] += s[d];
    s[b] = Integer.rotateLeft(s[b] ^ s[c], 12);
    s[a] += s[b];
    s[d] = Integer.rotateLeft(s[d] ^ s[a], 8);
    s[c] += s[d];
    s[b] = Integer.rotateLeft(s[b] ^ s[c], 7);
  }

  private static int load32LittleEndian(byte[] src, int offset) {
    return (src[offset] & 0xff)
        | ((src[offset + 1] & 0xff) << 8)
        | ((src[offset + 2] & 0xff) << 16)
        | ((src[offset + 3] & 0xff) << 24);
  }

  private static void store32LittleEndian(int value, byte[] dst, int offset) {
    dst[offset] = (byte) value;
    dst[offset + 1] = (byte) (value >>> 8);
    dst[offset + 2] = (byte) (value >>> 16);
    dst[offset + 3] = (byte) (value >>> 24);
  }
}
