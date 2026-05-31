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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DefaultRetryPolicyTest {

  @Test
  void defaultPolicyIsOnlyAPassthrough() {
    DefaultRetryPolicy policy = new DefaultRetryPolicy();

    assertTrue(policy.shouldRetry(1, new IllegalStateException("failed")));
    assertTrue(policy.shouldRetry(Integer.MAX_VALUE, new RuntimeException("still failed")));
    assertEquals(Duration.ZERO, policy.getDelay(Integer.MAX_VALUE));
  }
}
