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
 * Thrown when a protected value cannot be decrypted: an AEAD authentication-tag mismatch, corrupted
 * ciphertext, or a key or additional-authenticated-data that does not match what the value was
 * bound to.
 *
 * <p><b>Poison data, non-retryable.</b> A retry cannot fix a value whose authentication fails — the
 * bytes are wrong, not temporarily unavailable. The runtime routes this to the controlled failure
 * path (FAILED/DLQ) rather than retrying, and a decryption failure on the execution path must never
 * leave a job claimed and stuck {@code RUNNING}.
 */
public class PayloadDecryptionException extends PayloadEncryptionException {

  @Serial private static final long serialVersionUID = 1L;

  public PayloadDecryptionException(String message) {
    super(message);
  }

  public PayloadDecryptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
