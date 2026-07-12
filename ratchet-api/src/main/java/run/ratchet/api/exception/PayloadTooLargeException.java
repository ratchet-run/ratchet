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
import run.ratchet.spi.PayloadSerializer;

/**
 * Thrown before submission when a serialized job payload exceeds the configured size limit.
 *
 * <p>The measured value is the UTF-8 byte length of the JSON returned by the active {@link
 * PayloadSerializer}. Encryption framing and database-specific storage overhead are not included.
 */
public class PayloadTooLargeException extends IllegalArgumentException {

  @Serial private static final long serialVersionUID = 1L;

  private final long actualBytes;
  private final long maxBytes;

  /**
   * Creates a payload-size rejection.
   *
   * @param actualBytes UTF-8 byte length of the serialized job payload
   * @param maxBytes configured maximum UTF-8 byte length
   */
  public PayloadTooLargeException(long actualBytes, long maxBytes) {
    super(
        "Serialized job payload is "
            + actualBytes
            + " UTF-8 bytes, exceeding the configured maximum of "
            + maxBytes
            + " bytes");
    this.actualBytes = actualBytes;
    this.maxBytes = maxBytes;
  }

  /** Returns the measured UTF-8 byte length. */
  public long actualBytes() {
    return actualBytes;
  }

  /** Returns the configured maximum UTF-8 byte length. */
  public long maxBytes() {
    return maxBytes;
  }
}
