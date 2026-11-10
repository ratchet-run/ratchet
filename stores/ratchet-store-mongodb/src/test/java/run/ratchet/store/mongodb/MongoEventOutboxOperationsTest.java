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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.*;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MongoEventOutboxOperationsTest {
  @ParameterizedTest
  @ValueSource(strings = {"event_type", "retry_attempt", "event_ts"})
  void malformedSnapshotPreservesIdAndDoesNotBlockLaterRows(String field) {
    Document malformed = row();
    switch (field) {
      case "event_type" -> malformed.put(field, "UNKNOWN_EVENT");
      case "retry_attempt" -> malformed.put(field, -1);
      case "event_ts" -> malformed.remove(field);
      default -> throw new AssertionError(field);
    }
    Document valid = row();
    var fixture = new Fixture(List.of(malformed, valid));

    var results = fixture.operations.findEventsCreatedBefore(Instant.now(), 100);

    assertEquals(2, results.size());
    assertEquals(malformed.get("_id"), results.get(0).eventId());
    assertNull(results.get(0).event());
    Class<? extends RuntimeException> expectedFailure =
        field.equals("event_ts") ? NullPointerException.class : IllegalArgumentException.class;
    assertInstanceOf(expectedFailure, results.get(0).decodingFailure());
    assertEquals(valid.get("_id"), results.get(1).eventId());
    assertEquals(valid.get("_id"), results.get(1).event().eventId());
    assertNull(results.get(1).decodingFailure());
  }

  @Test
  void unreadableEventIdFailsTheBatch() {
    Document malformed = row();
    malformed.put("_id", "not-a-uuid");
    var fixture = new Fixture(List.of(malformed, row()));
    assertThrows(
        ClassCastException.class,
        () -> fixture.operations.findEventsCreatedBefore(Instant.now(), 100));
  }

  @Test
  void databaseReadFailurePropagates() {
    var fixture = new Fixture(List.of(row()));
    var failure = new MongoException("offline");
    fixture.readFailure = failure;
    assertSame(
        failure,
        assertThrows(
            MongoException.class,
            () -> fixture.operations.findEventsCreatedBefore(Instant.now(), 100)));
  }

  @Test
  void databaseCursorFailureAfterReadingARowPropagates() {
    var fixture = new Fixture(List.of(row(), row()));
    var failure = new MongoException("connection lost");
    fixture.cursorFailure = failure;
    assertSame(
        failure,
        assertThrows(
            MongoException.class,
            () -> fixture.operations.findEventsCreatedBefore(Instant.now(), 100)));
  }

  private static final class Fixture {
    final MongoEventOutboxOperations operations;
    RuntimeException readFailure;
    RuntimeException cursorFailure;

    Fixture(List<Document> rows) {
      AtomicInteger index = new AtomicInteger();
      MongoCursor<Document> cursor =
          proxy(
              MongoCursor.class,
              (proxy, method, args) ->
                  switch (method.getName()) {
                    case "hasNext" -> {
                      if (index.get() > 0 && cursorFailure != null) throw cursorFailure;
                      yield index.get() < rows.size();
                    }
                    case "next" -> rows.get(index.getAndIncrement());
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                  });
      FindIterable<Document> query =
          proxy(
              FindIterable.class,
              (proxy, method, args) ->
                  switch (method.getName()) {
                    case "sort", "limit" -> proxy;
                    case "iterator" -> cursor;
                    default -> throw new UnsupportedOperationException(method.getName());
                  });
      MongoCollection<Document> collection =
          proxy(
              MongoCollection.class,
              (proxy, method, args) -> {
                if (method.getName().equals("find")) {
                  if (readFailure != null) throw readFailure;
                  return query;
                }
                throw new UnsupportedOperationException(method.getName());
              });
      MongoDatabase database =
          proxy(
              MongoDatabase.class,
              (proxy, method, args) -> {
                if (method.getName().equals("getCollection")) return collection;
                throw new UnsupportedOperationException(method.getName());
              });
      MongoClient client =
          proxy(
              MongoClient.class,
              (proxy, method, args) -> {
                throw new AssertionError("Read must not open a transaction");
              });
      operations = new MongoEventOutboxOperations(new MongoStoreContext(client, database));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<?> type, InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
  }

  private static Document row() {
    return new Document("_id", UUID.randomUUID())
        .append("event_type", "JOB_COMPLETED")
        .append("job_id", UUID.randomUUID())
        .append("job_type", "SINGLE")
        .append("event_ts", Date.from(Instant.parse("2026-01-01T00:00:00Z")))
        .append("execution_time_ms", 7L)
        .append("retry_attempt", 0)
        .append("created_at", Date.from(Instant.parse("2026-01-01T00:00:00Z")));
  }
}
