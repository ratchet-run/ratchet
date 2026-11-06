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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class SqlEventOutboxTest {
  @Test
  void emptyDeleteDoesNotContactDatabase() {
    var em = mock(EntityManager.class);
    assertEquals(0, SqlEventOutbox.delete(em, List.of(), id -> id));
    verifyNoInteractions(em);
  }

  @Test
  void invalidBoundsAreRejectedBeforeDatabaseCalls() {
    var em = mock(EntityManager.class);
    List<UUID> ids = IntStream.range(0, 501).mapToObj(i -> UUID.randomUUID()).toList();
    assertThrows(IllegalArgumentException.class, () -> SqlEventOutbox.delete(em, ids, id -> id));
    assertThrows(IllegalArgumentException.class, () -> SqlEventOutbox.find(em, Instant.now(), 0));
    verifyNoInteractions(em);
  }
}
