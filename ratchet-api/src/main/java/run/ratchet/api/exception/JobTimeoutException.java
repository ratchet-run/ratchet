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
 * Thrown when a job exceeds its configured execution timeout.
 *
 * <p>Distinct from {@link InterruptedException} so that timeout-vs-interrupt failures can be
 * disambiguated by metrics, logs, and {@code shouldNotRetry} policies. Previously the runtime
 * inferred timeouts from {@code InterruptedException}, which conflated genuine cancellation,
 * cooperative shutdown, and watchdog timeouts.
 */
public class JobTimeoutException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public JobTimeoutException(String message) {
    super(message);
  }

  public JobTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
