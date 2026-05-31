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
 * Thrown when a resilience strategy rejects a protected call because the circuit is open.
 *
 * <p>This exception identifies scheduler back-pressure caused by an unavailable protected service.
 * It is distinct from task failure: callers that catch it should reschedule or delay the work
 * without consuming a job retry attempt.
 */
public class CircuitBreakerOpenException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public CircuitBreakerOpenException(String message) {
    super(message);
  }

  public CircuitBreakerOpenException(String message, Throwable cause) {
    super(message, cause);
  }
}
