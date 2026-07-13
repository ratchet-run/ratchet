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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JobPriorityPersistedCodeTest {

  private static final Map<JobPriority, Integer> FROZEN_CODES =
      Map.of(
          JobPriority.LOWEST, 0,
          JobPriority.LOW, 1,
          JobPriority.NORMAL, 2,
          JobPriority.HIGH, 3,
          JobPriority.CRITICAL, 4);

  @Test
  void persistedCodesMatchEveryExistingOnDiskValue() {
    assertEquals(Set.of(JobPriority.values()), FROZEN_CODES.keySet());
    assertEquals(FROZEN_CODES.size(), Set.copyOf(FROZEN_CODES.values()).size());

    FROZEN_CODES.forEach(
        (priority, code) -> {
          assertEquals(code, priority.persistedCode(), priority.name());
          assertEquals(priority, JobPriority.fromPersistedCode(code));
          assertEquals(priority, JobPriority.findByPersistedCode(code).orElseThrow());
        });
  }

  @Test
  void lookupRejectsUnknownPersistedCodes() {
    IllegalArgumentException negative =
        assertThrows(IllegalArgumentException.class, () -> JobPriority.fromPersistedCode(-1));
    IllegalArgumentException unknown =
        assertThrows(IllegalArgumentException.class, () -> JobPriority.fromPersistedCode(99));

    assertEquals("Unknown persisted JobPriority code: -1", negative.getMessage());
    assertEquals("Unknown persisted JobPriority code: 99", unknown.getMessage());
    assertFalse(JobPriority.findByPersistedCode(-1).isPresent());
    assertTrue(JobPriority.findByPersistedCode(JobPriority.NORMAL.persistedCode()).isPresent());
  }
}
