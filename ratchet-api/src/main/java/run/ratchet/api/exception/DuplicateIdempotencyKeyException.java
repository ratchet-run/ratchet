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
 * Thrown by a store when an insert collides with the unique constraint on a job's idempotency key.
 *
 * <p>This signals a concurrent submission that lost the race to insert. The scheduler converges to
 * the documented idempotent result by re-resolving the existing job and returning its handle, so
 * application code does not normally observe this exception.
 */
public class DuplicateIdempotencyKeyException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  private final String idempotencyKey;

  public DuplicateIdempotencyKeyException(String idempotencyKey, Throwable cause) {
    super("Idempotency key already in use: " + idempotencyKey, cause);
    this.idempotencyKey = idempotencyKey;
  }

  /** Returns the idempotency key that triggered the unique-constraint violation. */
  public String idempotencyKey() {
    return idempotencyKey;
  }
}
