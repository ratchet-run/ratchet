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
 * Thrown by a {@code KeyProvider} when a key id read from a stored envelope is unknown to the
 * provider — typically a key retired before every row that referenced it had drained.
 *
 * <p><b>Poison data, non-retryable.</b> The key the value was encrypted under no longer exists, so
 * the value cannot be recovered until it is restored: the runtime routes the value to the controlled
 * failure path (DLQ) rather than retrying. The remediation is operational — re-add the missing key
 * to the provider and replay the affected jobs from the DLQ — which is why key retirement is gated
 * on a drain check.
 *
 * <p>Distinct from {@link KeyProviderUnavailableException}: a transiently unreachable provider may
 * know the key perfectly well and should be retried, whereas this exception means the id is
 * genuinely absent.
 */
public class KeyNotFoundException extends PayloadEncryptionException {

  @Serial private static final long serialVersionUID = 1L;

  public KeyNotFoundException(String message) {
    super(message);
  }

  public KeyNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
