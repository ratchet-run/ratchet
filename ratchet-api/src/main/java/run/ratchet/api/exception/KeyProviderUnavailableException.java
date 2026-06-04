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
 * Thrown by a {@code KeyProvider} when it is transiently unreachable — for example a KMS or HSM
 * timeout, a 5xx response, or a dropped connection.
 *
 * <p><b>Transient, retryable.</b> The key still exists; the provider is just momentarily out of
 * reach. The runtime retries the operation with backoff rather than failing the job, because a
 * short key-service outage must not permanently lose work.
 *
 * <p>This exception intentionally does <em>not</em> extend {@link RatchetTransientStoreException}.
 * That store-transient path bypasses the attempt counter and would retry forever; a downed key
 * service must instead be retried under the normal bounded backoff so a prolonged outage eventually
 * surfaces rather than spinning indefinitely. Contrast with {@link KeyNotFoundException}, which
 * means the key is genuinely gone and is not retryable.
 */
public class KeyProviderUnavailableException extends PayloadEncryptionException {

  @Serial private static final long serialVersionUID = 1L;

  public KeyProviderUnavailableException(String message) {
    super(message);
  }

  public KeyProviderUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
