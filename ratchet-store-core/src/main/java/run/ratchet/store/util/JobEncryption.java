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
    return EncryptionHolder.encryptionActiveFor(job.isEncryptedPayload());
  }

  /**
   * The current key id to stamp on the row when it is encrypted, or {@code null} when it is not.
   * The id is recorded so a key cannot be retired while a live row still references it.
   */
  public static String keyId(boolean active) {
    return active ? EncryptionHolder.keyProvider().currentKey().keyId() : null;
  }
}
