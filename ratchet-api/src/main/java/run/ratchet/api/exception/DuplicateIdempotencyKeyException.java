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
 * Thrown by a store when a job's idempotency key is already reserved.
 *
 * <p>Single-job creation leaves the caller's transaction usable and writes nothing for the job.
 * Submission resolves the reservation to the original job handle, including after retained job
 * history has been deleted. This exception escapes submission only when the original cannot be
 * resolved.
 */
public class DuplicateIdempotencyKeyException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  private final String idempotencyKey;

  public DuplicateIdempotencyKeyException(String idempotencyKey) {
    this(idempotencyKey, null);
  }

  public DuplicateIdempotencyKeyException(String idempotencyKey, Throwable cause) {
    super("Idempotency key already in use: " + idempotencyKey, cause);
    this.idempotencyKey = idempotencyKey;
  }

  /** Returns the idempotency key that is already reserved. */
  public String idempotencyKey() {
    return idempotencyKey;
  }
}
