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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SqlEventOutboxTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 8, 11})
  void malformedSnapshotPreservesIdAndDoesNotBlockLaterRows(int column) {
    Object[] malformed = row();
    malformed[column] =
        switch (column) {
          case 1 -> "UNKNOWN_EVENT";
          case 8 -> null;
          case 11 -> -1;
          default -> throw new AssertionError(column);
        };
    Object[] valid = row();
    var em = databaseRows(malformed, valid);

    var results = SqlEventOutbox.find(em, Instant.now(), 100);

    assertEquals(2, results.size());
    assertEquals(malformed[0], results.get(0).eventId());
    assertNull(results.get(0).event());
    Class<? extends RuntimeException> expectedFailure =
        column == 8 ? NullPointerException.class : IllegalArgumentException.class;
    assertInstanceOf(expectedFailure, results.get(0).decodingFailure());
    assertEquals(valid[0], results.get(1).eventId());
    assertEquals(valid[0], results.get(1).event().eventId());
    assertNull(results.get(1).decodingFailure());
  }

  @Test
  void unreadableEventIdFailsTheBatch() {
    Object[] malformed = row();
    malformed[0] = "not-a-uuid";
    var em = databaseRows(malformed, row());
    assertThrows(IllegalArgumentException.class, () -> SqlEventOutbox.find(em, Instant.now(), 100));
  }

  @Test
  void databaseReadFailurePropagates() {
    var em = mock(EntityManager.class);
    var query = mock(Query.class, RETURNS_SELF);
    when(em.createNativeQuery(anyString())).thenReturn(query);
    var failure = new PersistenceException("offline");
    when(query.getResultList()).thenThrow(failure);
    assertSame(
        failure,
        assertThrows(
            PersistenceException.class, () -> SqlEventOutbox.find(em, Instant.now(), 100)));
  }

  @Test
  void databaseLobReadFailurePropagates() throws SQLException {
    Object[] malformed = row();
    var lob = mock(Clob.class);
    var failure = new SQLException("connection lost");
    when(lob.getCharacterStream()).thenThrow(failure);
    malformed[10] = lob;
    var em = databaseRows(malformed, row());
    assertSame(
        failure,
        assertThrows(IllegalStateException.class, () -> SqlEventOutbox.find(em, Instant.now(), 100))
            .getCause());
  }

  private static EntityManager databaseRows(Object[]... rows) {
    var em = mock(EntityManager.class);
    var query = mock(Query.class, RETURNS_SELF);
    when(em.createNativeQuery(anyString())).thenReturn(query);
    when(query.getResultList()).thenReturn(List.of(rows));
    return em;
  }

  private static Object[] row() {
    return new Object[] {
      UUID.randomUUID(),
      "JOB_COMPLETED",
      UUID.randomUUID(),
      null,
      null,
      "SINGLE",
      null,
      "dead-node",
      Timestamp.from(Instant.parse("2026-01-01T00:00:00Z")),
      7L,
      null,
      0,
      Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"))
    };
  }

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
