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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ResourceLimitTest {

  @Test
  void ofUsesTheDefaultRetryDelayAndNoDescription() {
    ResourceLimit limit = ResourceLimit.of("gpu", 2);

    assertEquals("gpu", limit.resourceName());
    assertEquals(2, limit.maxConcurrent());
    assertEquals(Duration.ofSeconds(5), ResourceLimit.DEFAULT_RETRY_DELAY);
    assertEquals(ResourceLimit.DEFAULT_RETRY_DELAY, limit.retryDelay());
    assertNull(limit.description());
  }

  @Test
  void withersReturnChangedCopies() {
    ResourceLimit base = ResourceLimit.of("gpu", 2);

    ResourceLimit changed =
        base.withRetryDelay(Duration.ofMillis(1500)).withDescription("GPU pool");

    assertEquals(new ResourceLimit("gpu", 2, Duration.ofMillis(1500), "GPU pool"), changed);
    assertEquals(new ResourceLimit("gpu", 2, ResourceLimit.DEFAULT_RETRY_DELAY, null), base);
    assertNull(changed.withDescription(null).description());
  }

  @Test
  void constructorRejectsNullNameAndNullRetryDelay() {
    assertThrows(NullPointerException.class, () -> ResourceLimit.of(null, 1));
    assertThrows(NullPointerException.class, () -> new ResourceLimit("gpu", 1, null, "GPU pool"));
    assertThrows(NullPointerException.class, () -> ResourceLimit.of("gpu", 1).withRetryDelay(null));
  }

  @Test
  void constructorKeepsValuesThatOnlySetResourceLimitRejects() {
    // A store reads rows that may have been written outside Ratchet, so the record itself does
    // not apply the range rules. setResourceLimit does.
    assertDoesNotThrow(() -> new ResourceLimit(" ", 0, Duration.ofMillis(-1), "é"));
  }
}
