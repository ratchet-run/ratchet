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
import run.ratchet.api.IdempotencyKeyHolder.Retention;

class IdempotencyKeyHolderTest {
  @Test
  void rejectsNullIdentityAndRetention() {
    UUID id = UUID.randomUUID();
    assertThrows(
        NullPointerException.class,
        () -> new IdempotencyKeyHolder(null, Retention.PURGED, null, null));
    assertThrows(NullPointerException.class, () -> new IdempotencyKeyHolder(id, null, null, null));
  }

  @Test
  void retainedRowsRequireTypeAndStatus() {
    UUID id = UUID.randomUUID();
    for (Retention retention : new Retention[] {Retention.LIVE, Retention.ARCHIVED}) {
      assertThrows(
          NullPointerException.class,
          () -> new IdempotencyKeyHolder(id, retention, null, JobStatus.FAILED));
      assertThrows(
          NullPointerException.class,
          () -> new IdempotencyKeyHolder(id, retention, JobType.SINGLE, null));
    }
  }

  @Test
  void purgedRowsRequireNullTypeAndStatus() {
    UUID id = UUID.randomUUID();
    assertEquals(id, new IdempotencyKeyHolder(id, Retention.PURGED, null, null).id());
    assertThrows(
        IllegalArgumentException.class,
        () -> new IdempotencyKeyHolder(id, Retention.PURGED, JobType.SINGLE, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new IdempotencyKeyHolder(id, Retention.PURGED, null, JobStatus.FAILED));
    assertThrows(
        IllegalArgumentException.class,
        () -> new IdempotencyKeyHolder(id, Retention.PURGED, JobType.SINGLE, JobStatus.FAILED));
  }

  @Test
  void equalityUsesAllComponents() {
    UUID id = UUID.randomUUID();
    var holder = new IdempotencyKeyHolder(id, Retention.LIVE, JobType.SINGLE, JobStatus.FAILED);
    var equal = new IdempotencyKeyHolder(id, Retention.LIVE, JobType.SINGLE, JobStatus.FAILED);
    assertEquals(holder, equal);
    assertEquals(holder.hashCode(), equal.hashCode());
    assertNotEquals(
        holder,
        new IdempotencyKeyHolder(
            UUID.randomUUID(), Retention.LIVE, JobType.SINGLE, JobStatus.FAILED));
    assertNotEquals(
        holder, new IdempotencyKeyHolder(id, Retention.ARCHIVED, JobType.SINGLE, JobStatus.FAILED));
    assertNotEquals(
        holder, new IdempotencyKeyHolder(id, Retention.LIVE, JobType.BATCH, JobStatus.FAILED));
    assertNotEquals(
        holder, new IdempotencyKeyHolder(id, Retention.LIVE, JobType.SINGLE, JobStatus.SUCCEEDED));
  }
}
