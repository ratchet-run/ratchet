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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;

/**
 * The versioned, self-describing envelope the framework wraps around each AEAD ciphertext.
 *
 * <p>The framework — not the engine — owns this framing. {@code rcph:e:} is a <b>reserved,
 * version-independent marker prefix</b>: a stored value is detected as a ciphertext frame when it
 * begins with the marker (a prefix check, see {@link #isFramed}), and the framework's encode path
 * is the only writer that emits the prefix, so plaintext and encrypted rows coexist safely during a
 * rollout. The marker is a commitment: a value that carries it but does not parse as a well-formed
 * frame is corrupt ciphertext (poison), not plaintext, and surfaces as {@link
 * PayloadDecryptionException} rather than passing silently through to a confusing deserialization
 * error downstream. A legacy value that merely begins with the reserved prefix is therefore read as
 * poison, not silently as plaintext — fail-closed, not fail-open.
 *
 * <p><b>Forward compatibility.</b> The version lives in the framed body, not the marker, so a value
 * written by a newer Ratchet (a higher version) is still <em>detected</em> as a frame and fails
 * closed with {@link UnsupportedEnvelopeVersionException} — it is never mistaken for plaintext and
 * passed through. A version-bearing marker (e.g. {@code rcph:1:}) would fail that detection for a
 * future {@code rcph:2:} value and read it as cleartext, which is why the marker is
 * version-independent. The runtime treats that exception as upgrade-pending (release the claim,
 * retry on an upgraded peer), not as poison.
 *
 * <p><b>Binary layout</b> (base64url-encoded after the {@code rcph:e:} marker):
 *
 * <pre>
 *   version       : 1 byte  (currently {@value #VERSION})
 *   algorithmIdLen : 4 bytes (big-endian int)   ┐
 *   algorithmId    : N bytes (UTF-8)             │ canonical header — drives read-time engine
 *   keyIdLen       : 4 bytes                     │ dispatch and key resolution, and is folded into
 *   keyId          : N bytes (UTF-8)             │ the AEAD additional-authenticated-data so it
 *   wrappedKeyLen  : 4 bytes                     │ cannot be tampered without failing the tag
 *   wrappedKey     : N bytes (empty for local providers; the wrapped DEK for KMS providers) ┘
 *   body           : remaining bytes (engine's opaque output: nonce ∥ ciphertext ∥ tag)
 * </pre>
 *
 * <p>The <b>wrapped-key</b> field carries the wrapped data-encryption key for a {@code
 * WrappedKeyProvider} (envelope encryption, e.g. KMS); local providers (static, environment
 * variable, JCA {@code KeyStore}) leave it empty. It is authenticated as part of the canonical
 * header, so it cannot be tampered without failing the AEAD tag.
 */
public final class EncryptionEnvelope {

  /** Reserved, version-independent marker prefix that commits a stored value to being a frame. */
  public static final String MARKER = "rcph:e:";

  /** Current envelope version written by {@link #canonicalHeader}. */
  public static final byte VERSION = 1;

  /**
   * Highest envelope version this node can read. Equal to {@link #VERSION} until a future version
   * is added; a value carrying a higher version is upgrade-pending, not poison.
   */
  public static final int MAX_READABLE_VERSION = VERSION;

  /**
   * Upper bound on the length-prefixed header fields, applied before allocation. The algorithm id
   * and key id are short identifiers; the wrapped key is at most a KMS-wrapped 256-bit DEK plus
   * provider framing. Generous caps that still reject a malicious or corrupt length prefix from
   * driving a large pre-authentication allocation.
   */
  private static final int MAX_ID_FIELD_SIZE = 1024;

  private static final int MAX_WRAPPED_KEY_SIZE = 4096;

  private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

  private EncryptionEnvelope() {}

  /**
   * A decoded envelope: the routing header fields, the canonical header bytes (for AAD
   * reconstruction), and the opaque engine body.
   *
   * @param algorithmId selects the engine that must decrypt the body
   * @param keyId selects the key the body was encrypted under
   * @param wrappedKey the wrapped-DEK blob (empty for local providers)
   * @param body the engine's opaque AEAD output
   * @param canonicalHeader the exact header bytes, re-fed into AAD on decrypt
   */
  public record Frame(
      String algorithmId, String keyId, byte[] wrappedKey, byte[] body, byte[] canonicalHeader) {}

  /**
   * Builds the canonical header bytes for a write. The caller needs these before encrypting,
   * because they are part of the AEAD additional-authenticated-data the engine binds the ciphertext
   * to.
   *
   * @param algorithmId the writing engine's algorithm id; must not be {@code null}
   * @param keyId the writing key's id; must not be {@code null}
   * @param wrappedKey the wrapped-DEK blob, or {@code null}/empty when the provider does not use
   *     one
   * @return the canonical header bytes
   */
  public static byte[] canonicalHeader(String algorithmId, String keyId, byte[] wrappedKey) {
    byte[] algo = algorithmId.getBytes(UTF_8);
    byte[] key = keyId.getBytes(UTF_8);
    byte[] wrapped = wrappedKey == null ? new byte[0] : wrappedKey;
    return ByteBuffer.allocate(1 + 4 + algo.length + 4 + key.length + 4 + wrapped.length)
        .put(VERSION)
        .putInt(algo.length)
        .put(algo)
        .putInt(key.length)
        .put(key)
        .putInt(wrapped.length)
        .put(wrapped)
        .array();
  }

  /**
   * Encodes a complete stored value from the canonical header (see {@link #canonicalHeader}) and
   * the engine body produced under that header.
   *
   * @param canonicalHeader the header bytes used as AAD when {@code body} was produced
   * @param body the engine's opaque AEAD output
   * @return the {@code rcph:e:}-marked, base64url-encoded stored value
   */
  public static String encode(byte[] canonicalHeader, byte[] body) {
    byte[] full = Arrays.copyOf(canonicalHeader, canonicalHeader.length + body.length);
    System.arraycopy(body, 0, full, canonicalHeader.length, body.length);
    return MARKER + ENCODER.encodeToString(full);
  }

  /**
   * Reports whether a stored value carries the frame marker. A quick prefix check; full validity is
   * established by {@link #decode(String)}.
   */
  public static boolean isFramed(String stored) {
    return stored != null && stored.startsWith(MARKER);
  }

  /**
   * Decodes a stored value.
   *
   * @param stored the stored column/field value
   * @return the decoded {@link Frame}, or {@code null} when {@code stored} is not a frame (legacy
   *     plaintext — left for the caller to pass through)
   * @throws PayloadDecryptionException when the value carries the marker but is not a well-formed
   *     frame (corrupt or truncated ciphertext, or an invalid version)
   * @throws UnsupportedEnvelopeVersionException when the value carries a version newer than this
   *     node can read (written by a newer Ratchet) — upgrade-pending, not poison
   */
  public static Frame decode(String stored) {
    if (!isFramed(stored)) {
      return null;
    }
    byte[] full;
    try {
      full = DECODER.decode(stored.substring(MARKER.length()));
    } catch (IllegalArgumentException e) {
      throw new PayloadDecryptionException("Corrupt encryption envelope: invalid base64 body", e);
    }
    ByteBuffer buf = ByteBuffer.wrap(full);
    int version;
    try {
      version = buf.get();
    } catch (BufferUnderflowException e) {
      throw new PayloadDecryptionException("Corrupt or truncated encryption envelope", e);
    }
    if (version <= 0) {
      throw new PayloadDecryptionException(
          "Corrupt encryption envelope: invalid version " + version);
    }
    if (version > MAX_READABLE_VERSION) {
      throw new UnsupportedEnvelopeVersionException(version, MAX_READABLE_VERSION);
    }
    // Only v1 exists today; a future version adds a branch here keyed on `version`.
    return decodeV1(full, buf);
  }

  private static Frame decodeV1(byte[] full, ByteBuffer buf) {
    try {
      String algorithmId = readLengthPrefixedString(buf, MAX_ID_FIELD_SIZE);
      String keyId = readLengthPrefixedString(buf, MAX_ID_FIELD_SIZE);
      byte[] wrappedKey = readLengthPrefixedBytes(buf, MAX_WRAPPED_KEY_SIZE);
      byte[] canonicalHeader = Arrays.copyOfRange(full, 0, buf.position());
      byte[] body = new byte[buf.remaining()];
      buf.get(body);
      return new Frame(algorithmId, keyId, wrappedKey, body, canonicalHeader);
    } catch (BufferUnderflowException | IllegalArgumentException e) {
      throw new PayloadDecryptionException("Corrupt or truncated encryption envelope", e);
    }
  }

  private static byte[] readLengthPrefixedBytes(ByteBuffer buf, int maxLen) {
    int len = buf.getInt();
    if (len < 0 || len > maxLen || len > buf.remaining()) {
      throw new IllegalArgumentException("Invalid envelope field length: " + len);
    }
    byte[] out = new byte[len];
    buf.get(out);
    return out;
  }

  private static String readLengthPrefixedString(ByteBuffer buf, int maxLen) {
    return new String(readLengthPrefixedBytes(buf, maxLen), UTF_8);
  }
}
