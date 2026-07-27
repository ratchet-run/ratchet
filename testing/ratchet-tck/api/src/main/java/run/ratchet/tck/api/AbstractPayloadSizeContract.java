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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadTooLargeException;

/** Public-API contract for implementations that advertise a serialized job-payload size limit. */
public abstract class AbstractPayloadSizeContract {

  private long maxPayloadBytes;

  @BeforeEach
  void requirePayloadLimit() {
    Assumptions.assumeTrue(
        runtime().maxPayloadBytes().isPresent(),
        "AbstractPayloadSizeContract requires RatchetTckRuntime.maxPayloadBytes()");
    maxPayloadBytes = runtime().maxPayloadBytes().orElseThrow();
    Assumptions.assumeTrue(
        maxPayloadBytes < Integer.MAX_VALUE - 1L,
        "TCK cannot allocate a payload larger than the implementation's configured limit");
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void oversizedSerializedPayloadIsRejectedAtSubmission() {
    String value = "x".repeat(Math.toIntExact(maxPayloadBytes + 1));

    PayloadTooLargeException exception =
        assertThrows(
            PayloadTooLargeException.class,
            // Intentional: captures oversized value into payload; excluded from native TCK subset.
            () -> runtime().scheduler().enqueue(() -> TckJobs.acceptPayload(value)).submit());

    assertEquals(maxPayloadBytes, exception.maxBytes());
    assertTrue(exception.actualBytes() > maxPayloadBytes);
  }

  protected abstract RatchetTckRuntime runtime();
}
