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
 * Thrown when a persisted signal outcome is not recognized while reconstructing a signal decision.
 *
 * <p>This indicates corrupt or incompatible durable state, so retrying the same row cannot recover.
 * The runtime routes the job directly to the controlled failure path instead of consuming retry
 * attempts.
 */
@DoNotRetry("An unrecognized persisted signal outcome cannot become valid on retry")
public class SignalOutcomeHydrationException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  private final UUID jobId;
  private final String persistedOutcome;

  /**
   * Creates a signal-outcome hydration failure.
   *
   * @param jobId job whose persisted signal decision could not be reconstructed
   * @param persistedOutcome unrecognized value read from durable storage
   * @param cause enum conversion failure
   */
  public SignalOutcomeHydrationException(
      UUID jobId, String persistedOutcome, IllegalArgumentException cause) {
    super(
        "Failed to hydrate signal outcome for job "
            + jobId
            + ": persisted value '"
            + persistedOutcome
            + "' is not a recognized SignalDecision.Outcome",
        cause);
    this.jobId = jobId;
    this.persistedOutcome = persistedOutcome;
  }

  /** Returns the job whose signal decision could not be reconstructed. */
  public UUID getJobId() {
    return jobId;
  }

  /** Returns the unrecognized value read from durable storage. */
  public String getPersistedOutcome() {
    return persistedOutcome;
  }
}
