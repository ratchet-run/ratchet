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
package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobInvocationTest {
  @Test
  void nullableArgumentsArePreservedInAnImmutableDefensiveCopy() {
    List<Object> args = new ArrayList<>(Arrays.asList("first", null, 3L));
    JobInvocation invocation = new JobInvocation("Target", "run", "", true, args);
    args.set(0, "changed");
    assertEquals(Arrays.asList("first", null, 3L), invocation.arguments());
    assertThrows(UnsupportedOperationException.class, () -> invocation.arguments().set(1, "x"));
    assertTrue(new JobInvocation("Target", "run", "", true, null).arguments().isEmpty());
  }
}
