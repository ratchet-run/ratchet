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
 * Base type for the payload-encryption exception family.
 *
 * <p>The subtypes carry the distinction the runtime routes on: {@link PayloadDecryptionException}
 * and {@link KeyNotFoundException} are poison data that no retry can fix and route to the controlled
 * failure path (DLQ); {@link KeyProviderUnavailableException} is a transient infrastructure fault
 * that is retried with backoff; {@link EncryptionConfigurationException} is an operator mistake that
 * fails the node at startup. Code that only needs to know "an encryption operation failed," without
 * caring which kind, can catch this base type.
 *
 * <p>This family deliberately does <em>not</em> extend {@link RatchetTransientStoreException}: that
 * path bypasses the attempt counter, so classifying a decryption or key failure as transient-store
 * would spin forever instead of either retrying with backoff or routing poison data to DLQ.
 */
public class PayloadEncryptionException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public PayloadEncryptionException(String message) {
    super(message);
  }

  public PayloadEncryptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
