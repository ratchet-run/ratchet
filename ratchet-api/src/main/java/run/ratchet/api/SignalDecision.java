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
package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * Structured decision delivered to a signal-waiting job.
 *
 * <p>Approval and rejection are scheduler-visible metadata for audit, metrics, and events. They do
 * not by themselves decide job success or failure: a delivered decision still unblocks the job from
 * WAITING to PENDING, and the job body reads the decision from {@link
 * JobContext#signalPayload(Class)} and applies domain-specific behavior.
 *
 * @since 0.1
 */
@Incubating
public record SignalDecision(Outcome outcome, Serializable payload, String rejectionReason)
    implements Serializable {

  @Serial private static final long serialVersionUID = 8364271059123847041L;

  public SignalDecision {
    if (outcome == null) {
      throw new IllegalArgumentException("outcome must not be null");
    }
    rejectionReason =
        rejectionReason == null || rejectionReason.isBlank() ? null : rejectionReason.trim();
    if (outcome == Outcome.APPROVED && rejectionReason != null) {
      throw new IllegalArgumentException("approved decisions cannot include a rejection reason");
    }
    if (outcome == Outcome.REJECTED && rejectionReason == null) {
      throw new IllegalArgumentException("rejected decisions must include a rejection reason");
    }
  }

  /**
   * Creates an approval decision.
   *
   * @param payload optional serializable payload exposed to the unblocked job
   * @return an approved signal decision with no rejection reason
   */
  public static SignalDecision approved(@Nullable Serializable payload) {
    return new SignalDecision(Outcome.APPROVED, payload, null);
  }

  /**
   * Creates a rejection decision.
   *
   * @param payload optional serializable payload
   * @param rejectionReason non-blank human-readable rejection reason; leading and trailing
   *     whitespace is ignored
   * @return a rejected signal decision
   * @throws IllegalArgumentException if {@code rejectionReason} is null or blank
   */
  public static SignalDecision rejected(@Nullable Serializable payload, String rejectionReason) {
    return new SignalDecision(Outcome.REJECTED, payload, rejectionReason);
  }

  public boolean isApproved() {
    return outcome == Outcome.APPROVED;
  }

  public boolean isRejected() {
    return outcome == Outcome.REJECTED;
  }

  /**
   * Returns the optional payload cast to the requested type.
   *
   * @param type expected payload type
   * @param <T> expected payload type
   * @return the payload cast to {@code type}, or {@code null} when no payload was supplied
   * @throws ClassCastException if a non-null payload is not assignable to {@code type}
   */
  public <T> T payload(Class<T> type) {
    return payload == null ? null : type.cast(payload);
  }

  public enum Outcome {
    APPROVED,
    REJECTED
  }
}
