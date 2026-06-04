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

import java.nio.ByteBuffer;
import java.util.UUID;
import run.ratchet.spi.ProtectedSurface;

/**
 * Computes the final additional-authenticated-data (AAD) bytes the framework binds each ciphertext
 * to. The framework owns this computation so that read and write reconstruct identical bytes by
 * construction; the engine consumes the result verbatim and never recomputes it.
 *
 * <p>The AAD binds three things, each length-prefixed so the concatenation is injective (no {@code
 * surface="AB"||binding="C"} colliding with {@code surface="A"||binding="BC"}):
 *
 * <ol>
 *   <li>the envelope's <b>canonical header</b> (version, algorithm id, key id, wrapped key), so a
 *       tampered or corrupted routing field fails the AEAD tag instead of silently redirecting
 *       dispatch or key resolution;
 *   <li>the <b>protected surface</b>, so a ciphertext cannot be moved between surfaces;
 *   <li>a per-surface <b>binding</b> identity, so a ciphertext cannot be relocated between rows
 *       that share a surface.
 * </ol>
 *
 * <p>The binding identity depends on the surface and is supplied by the caller:
 *
 * <ul>
 *   <li>row-bound surfaces (payload args, parameter values, result, callback payloads) bind the
 *       owning job id;
 *   <li>{@link ProtectedSurface#SIGNAL_PAYLOAD} binds the signal key — a broadcast writes one
 *       ciphertext to every waiting row matching the key, so binding the job id would fail the tag
 *       on all rows but one, whereas every targeted row shares the key;
 *   <li>{@link ProtectedSurface#WORKFLOW_CONDITION_PREDICATE} binds the parent job id, which is in
 *       scope at both the write site and the evaluation site.
 * </ul>
 */
public final class EncryptionAad {

  private EncryptionAad() {}

  /**
   * Computes the AAD bytes from the envelope header, the surface, and the surface's binding bytes.
   *
   * @param canonicalHeader the envelope's canonical header bytes (see {@link
   *     EncryptionEnvelope#canonicalHeader})
   * @param surface the protected surface; must not be {@code null}
   * @param binding the per-surface binding bytes (see {@link #binding(UUID)} / {@link
   *     #binding(String)}); never {@code null} (may be empty)
   * @return the final AAD bytes
   */
  public static byte[] compute(byte[] canonicalHeader, ProtectedSurface surface, byte[] binding) {
    byte[] surfaceBytes = surface.name().getBytes(UTF_8);
    return ByteBuffer.allocate(
            4 + canonicalHeader.length + 4 + surfaceBytes.length + 4 + binding.length)
        .putInt(canonicalHeader.length)
        .put(canonicalHeader)
        .putInt(surfaceBytes.length)
        .put(surfaceBytes)
        .putInt(binding.length)
        .put(binding)
        .array();
  }

  /**
   * Returns binding bytes for an id-bound surface, or empty bytes for a {@code null} id.
   *
   * @param id the binding id (job id or parent job id), or {@code null}
   * @return the canonical binding bytes
   */
  public static byte[] binding(UUID id) {
    return id == null ? new byte[0] : id.toString().getBytes(UTF_8);
  }

  /**
   * Returns binding bytes for a key-bound surface (the signal key), or empty bytes for a {@code
   * null} key.
   *
   * @param key the binding key, or {@code null}
   * @return the canonical binding bytes
   */
  public static byte[] binding(String key) {
    return key == null ? new byte[0] : key.getBytes(UTF_8);
  }
}
