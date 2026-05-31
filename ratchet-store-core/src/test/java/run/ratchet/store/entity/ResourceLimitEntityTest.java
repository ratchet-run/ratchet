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
package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class ResourceLimitEntityTest {

  @Test
  void defaultRetryDelayUsesNamedConstant() {
    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS, new ResourceLimitEntity().getRetryDelayMs());
  }

  @Test
  void equalityUsesResourceNameIdentity() {
    ResourceLimitEntity first = limit("database", 10);
    ResourceLimitEntity second = limit("database", 20);
    ResourceLimitEntity different = limit("api", 10);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, different);
  }

  private static ResourceLimitEntity limit(String resourceName, int maxConcurrent) {
    ResourceLimitEntity limit = new ResourceLimitEntity();
    limit.setResourceName(resourceName);
    limit.setMaxConcurrent(maxConcurrent);
    return limit;
  }
}
