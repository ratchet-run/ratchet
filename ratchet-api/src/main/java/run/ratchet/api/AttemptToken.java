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

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Identifies one claim of one job. The sequence grows only within a job, so compare tokens only for
 * the same job. A recurring schedule's firings are separate jobs; tokens do not order firings.
 *
 * <p>This value can be passed to other threads or stored next to application data. To fence your
 * own writes, store {@link #sequence()} with the data and refuse writes carrying a lower sequence.
 *
 * @since 0.6
 */
@Incubating
public record AttemptToken(UUID jobId, long sequence) implements Serializable {
  private static final long serialVersionUID = 1L;

  public AttemptToken {
    Objects.requireNonNull(jobId);
    if (sequence <= 0) {
      throw new IllegalArgumentException("sequence must be positive");
    }
  }

  /** Returns whether this token is newer than another token for the same job. */
  public boolean isNewerThan(AttemptToken other) {
    if (!jobId.equals(other.jobId())) {
      throw new IllegalArgumentException("Cannot compare tokens for different jobs");
    }
    return sequence > other.sequence();
  }

  @Override
  public String toString() {
    return jobId + ":" + sequence;
  }

  /** Parses the representation returned by {@link #toString()}. */
  public static AttemptToken parse(String text) {
    Objects.requireNonNull(text);
    int colon = text.indexOf(':');
    if (colon != 36 || colon != text.lastIndexOf(':')) {
      throw new IllegalArgumentException("Invalid attempt token");
    }
    AttemptToken token =
        new AttemptToken(
            UUID.fromString(text.substring(0, colon)), Long.parseLong(text.substring(colon + 1)));
    if (!token.toString().equals(text)) {
      throw new IllegalArgumentException("Invalid attempt token");
    }
    return token;
  }
}
