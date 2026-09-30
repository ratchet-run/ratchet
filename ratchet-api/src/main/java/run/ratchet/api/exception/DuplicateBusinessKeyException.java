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
 * Thrown by {@code submit()} when an active job (PENDING, RUNNING, PAUSED, or WAITING) already
 * holds the business key. Retrying will not succeed until that job reaches a terminal state.
 *
 * <p>This is distinct from {@link DuplicateIdempotencyKeyException}, where retrying in a fresh
 * transaction returns the original job's handle.
 */
public class DuplicateBusinessKeyException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  private final String businessKey;

  public DuplicateBusinessKeyException(String businessKey, String message) {
    super(message);
    this.businessKey = businessKey;
  }

  public DuplicateBusinessKeyException(String businessKey, String message, Throwable cause) {
    super(message, cause);
    this.businessKey = businessKey;
  }

  /** Returns the business key held by the existing active job. */
  public String businessKey() {
    return businessKey;
  }
}
