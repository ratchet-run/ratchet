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
import java.util.UUID;
import run.ratchet.api.DoNotRetry;

/**
 * Thrown when a {@link run.ratchet.spi.JobAuthorizationPolicy} denies a job operation.
 *
 * <p>Annotated {@link DoNotRetry} so that an authorization denial during execution does not consume
 * retry attempts — the denial is permanent and must be resolved by policy change, not by re-running
 * the same job.
 */
@DoNotRetry("Authorization denial is permanent; retrying will not change the outcome")
public class JobAuthorizationException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  private final UUID jobId;
  private final String operation;
  private final String principal;

  /**
   * Creates an authorization-denied exception.
   *
   * @param jobId job that was denied, or {@code null} when denial occurs before a job id exists
   * @param operation denied operation name, such as {@code create}, {@code cancel}, {@code
   *     replace}, or {@code execute}
   * @param principal caller principal, or {@code null} when no caller security context is active
   * @param message human-readable denial message
   */
  public JobAuthorizationException(UUID jobId, String operation, String principal, String message) {
    super(message);
    this.jobId = jobId;
    this.operation = operation;
    this.principal = principal;
  }

  /** The job that was denied. */
  public UUID getJobId() {
    return jobId;
  }

  /**
   * The operation that was denied: {@code create}, {@code cancel}, {@code pause}, {@code resume},
   * {@code retry}, {@code execute}, or {@code replace}.
   */
  public String getOperation() {
    return operation;
  }

  /** The principal that was denied, or {@code null} for system-initiated operations. */
  public String getPrincipal() {
    return principal;
  }
}
