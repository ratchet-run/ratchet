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

import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.WrappedKeyProvider;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;

/**
 * Resolves, once per write, whether a row is encrypted and under which key — so every store's write
 * path applies the same decision to both the encrypted surfaces and the {@code encrypted_payload} /
 * {@code encryption_key_id} columns. Keeping the two derivations together is what guarantees the
 * column never disagrees with whether the bytes are actually ciphertext.
 */
public final class JobEncryption {

  private JobEncryption() {}

  /**
   * Whether this row's protected surfaces must be encrypted: the global switch or the job's opt-in.
   * Fails loud (via {@link EncryptionHolder#encryptionActiveFor(boolean)}) when encryption is
   * wanted but no engine is installed.
   */
  public static boolean activeFor(JobEntity job) {
    return activeFor(job.isEncryptedPayload());
  }

  /**
   * Whether a write must be encrypted given the owning row's opt-in flag. Used by surfaces that
   * carry the flag outside a {@link JobEntity} (e.g. a recurring master template). Fails loud when
   * encryption is wanted but no engine is installed.
   */
  public static boolean activeFor(boolean encryptedPayload) {
    return EncryptionHolder.encryptionActiveFor(encryptedPayload);
  }

  /**
   * The current key id to stamp on the row when it is encrypted, or {@code null} when it is not.
   *
   * <p><b>Best-effort hint, not a drain oracle.</b> The authoritative key for any stored value is
   * the one named inside its self-describing envelope; this column is a denormalized, queryable
   * summary of the <em>most recently written</em> surface's write key. A single row can carry
   * surfaces written at different times (the payload at creation, a signal payload on delivery),
   * and under a future key rotation those can name a different key than this column. Treat it as a
   * coarse "is any row still plausibly on key K" filter, never as proof a key is safe to retire;
   * authoritative drain-checking arrives with the rotation tooling, which must scan envelopes
   * rather than trust this column.
   *
   * <p>A {@link WrappedKeyProvider} (KMS-style envelope encryption) leaves the hint {@code null}:
   * it has no single current key — it mints a fresh ephemeral DEK per write and identifies a row by
   * the master key id stored inside its self-describing envelope. Stamping a hint here would force
   * a throwaway DEK (a real KMS {@code GenerateDataKey} call) on every insert just to read an id,
   * with no value the envelope does not already carry.
   */
  public static String keyId(boolean active) {
    if (!active) {
      return null;
    }
    KeyProvider provider = EncryptionHolder.keyProvider();
    if (provider instanceof WrappedKeyProvider) {
      return null;
    }
    return provider.currentKey().keyId();
  }
}
