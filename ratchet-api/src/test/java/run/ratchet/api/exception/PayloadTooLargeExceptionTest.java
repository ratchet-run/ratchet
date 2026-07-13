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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PayloadTooLargeExceptionTest {

  @Test
  void exposesStableSizeDetailsAndMessage() {
    PayloadTooLargeException exception = new PayloadTooLargeException(1025, 1024);

    assertEquals(1025, exception.actualBytes());
    assertEquals(1024, exception.maxBytes());
    assertEquals(
        "Serialized job payload is 1025 UTF-8 bytes, exceeding the configured maximum of 1024 bytes",
        exception.getMessage());
  }
}
