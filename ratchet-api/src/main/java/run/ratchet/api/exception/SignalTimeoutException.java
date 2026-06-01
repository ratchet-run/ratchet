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
 * Thrown when a signal-waiting job exceeds its configured signal wait timeout.
 *
 * <p>The signal-timeout scanner does not raise this exception to callers directly. When the scanner
 * observes a WAITING job whose {@code signal_timeout_at} has elapsed, it transitions the job to
 * {@link run.ratchet.api.JobStatus#FAILED} with this exception's message recorded as the failure
 * reason and fires {@link run.ratchet.api.event.JobSignalTimedOutEvent}; the job is NOT left in
 * WAITING. The exception type exists so that internal failure-path code and observers can
 * distinguish timeout-driven transitions from task-thrown failures.
 *
 * <p>Unlike {@link RatchetTransientStoreException}, this is not retry-worthy at the infrastructure
 * level: the job has already been resolved to a terminal state. Reissuing the work requires
 * scheduling a new job (for example via {@link
 * run.ratchet.api.JobSchedulerService#retryJob(java.util.UUID)} once it has reached FAILED).
 */
public class SignalTimeoutException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public SignalTimeoutException(String message) {
    super(message);
  }

  public SignalTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
