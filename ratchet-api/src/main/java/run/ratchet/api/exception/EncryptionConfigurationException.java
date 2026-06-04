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
 * Thrown at bootstrap when encryption is enabled but cannot work as configured — no engine
 * installed, no key provider, or a provider that cannot produce a current key.
 *
 * <p><b>Configuration error: fail the node at startup.</b> The reference implementation's installer
 * raises this so the node refuses to start rather than begin polling with encryption that cannot
 * function. Jobs stay {@code PENDING} and run once the configuration is corrected; a deploy mistake
 * must never send jobs to the DLQ. This is distinct from the runtime failures in this family — it is
 * neither retried nor routed to a failure path, because there is no per-job operation in flight when
 * it fires.
 */
public class EncryptionConfigurationException extends PayloadEncryptionException {

  @Serial private static final long serialVersionUID = 1L;

  public EncryptionConfigurationException(String message) {
    super(message);
  }

  public EncryptionConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
