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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class BusinessKeyHolderTest {
  @Test
  void rejectsNullComponents() {
    UUID id = UUID.randomUUID();
    assertThrows(
        NullPointerException.class,
        () -> new BusinessKeyHolder(null, JobType.SINGLE, JobStatus.PENDING));
    assertThrows(
        NullPointerException.class, () -> new BusinessKeyHolder(id, null, JobStatus.PENDING));
    assertThrows(NullPointerException.class, () -> new BusinessKeyHolder(id, JobType.SINGLE, null));
  }

  @Test
  void equalityUsesAllComponents() {
    UUID id = UUID.randomUUID();
    var holder = new BusinessKeyHolder(id, JobType.SINGLE, JobStatus.PENDING);
    var equal = new BusinessKeyHolder(id, JobType.SINGLE, JobStatus.PENDING);
    assertEquals(holder, equal);
    assertEquals(holder.hashCode(), equal.hashCode());
    assertNotEquals(
        holder, new BusinessKeyHolder(UUID.randomUUID(), JobType.SINGLE, JobStatus.PENDING));
    assertNotEquals(holder, new BusinessKeyHolder(id, JobType.RECURRING, JobStatus.PENDING));
    assertNotEquals(holder, new BusinessKeyHolder(id, JobType.SINGLE, JobStatus.RUNNING));
  }
}
