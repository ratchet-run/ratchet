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
package run.ratchet.api.exception;

import java.io.Serial;

/**
 * Thrown when a stored encryption envelope carries a format version this node does not yet
 * understand — the value was written by a <em>newer</em> Ratchet during a rolling upgrade, before
 * this node was upgraded to read it.
 *
 * <p><b>Upgrade-pending, not poison.</b> Unlike {@link PayloadDecryptionException} (corrupt
 * ciphertext) or {@link KeyNotFoundException} (a key retired before its rows drained), a
 * future-version envelope is <em>valid data this node simply cannot read yet</em>. It becomes
 * readable the moment this node is upgraded, so the runtime must not dead-letter it. This exception
 * deliberately does <em>not</em> extend {@link PayloadDecryptionException}, so the poison-detection
 * path does not route it to the DLQ; instead the runtime releases the claim (returning the job to
 * the pending pool with backoff) so an already-upgraded peer can read it, and surfaces a metric so
 * a node stuck behind the fleet is visible.
 *
 * <p>This is the read-side half of the framework's forward-compatibility contract: the envelope
 * marker is version-independent, so a future version is always detected as a frame and fails
 * <em>closed</em> here rather than being mistaken for legacy plaintext and passed through.
 */
public class UnsupportedEnvelopeVersionException extends PayloadEncryptionException {

  @Serial private static final long serialVersionUID = 1L;

  private final int version;
  private final int maxSupportedVersion;

  /**
   * @param version the envelope version read from the stored value
   * @param maxSupportedVersion the highest version this node can read
   */
  public UnsupportedEnvelopeVersionException(int version, int maxSupportedVersion) {
    super(
        "Encryption envelope version "
            + version
            + " was written by a newer Ratchet; this node reads up to version "
            + maxSupportedVersion
            + ". Upgrade this node to drain these rows.");
    this.version = version;
    this.maxSupportedVersion = maxSupportedVersion;
  }

  /** Returns the envelope version that could not be read. */
  public int version() {
    return version;
  }

  /** Returns the highest envelope version this node can read. */
  public int maxSupportedVersion() {
    return maxSupportedVersion;
  }
}
