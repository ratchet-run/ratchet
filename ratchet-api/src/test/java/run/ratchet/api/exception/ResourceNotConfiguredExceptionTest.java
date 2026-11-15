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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.DoNotRetry;

class ResourceNotConfiguredExceptionTest {

  @Test
  void isANonRetryableIllegalArgumentThatSaysHowToFixIt() {
    ResourceNotConfiguredException exception = new ResourceNotConfiguredException("gpu");

    assertInstanceOf(IllegalArgumentException.class, exception);
    assertTrue(ResourceNotConfiguredException.class.isAnnotationPresent(DoNotRetry.class));
    assertEquals("gpu", exception.resourceName());
    assertEquals(
        "No limit is set for resource 'gpu'. Call"
            + " setResourceLimit(ResourceLimit.of(\"gpu\", maxConcurrent)) first, then re-run any"
            + " job that failed for this reason with retryJob.",
        exception.getMessage());
  }
}
