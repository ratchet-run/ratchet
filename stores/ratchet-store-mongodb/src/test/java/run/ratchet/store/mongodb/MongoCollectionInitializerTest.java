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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

class MongoCollectionInitializerTest {
  @Test
  @SuppressWarnings("unchecked")
  void missingLegacyIndexDuringConcurrentReplacementIsRetried() {
    MongoCollection<Document> collection = mock(MongoCollection.class);
    var initializer =
        new MongoCollectionInitializer(mock(MongoDatabase.class), mock(MongoClient.class));
    when(collection.createIndex(any(Bson.class), any(IndexOptions.class)))
        .thenThrow(commandFailure(85))
        .thenReturn("idx_job_depends_on");
    doThrow(commandFailure(27)).when(collection).dropIndex("idx_job_depends_on");

    assertDoesNotThrow(() -> initializer.createDependsOnIndex(collection));

    verify(collection).dropIndex("idx_job_depends_on");
    verify(collection, times(2)).createIndex(any(Bson.class), any(IndexOptions.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void otherDropFailuresPropagate() {
    MongoCollection<Document> collection = mock(MongoCollection.class);
    var initializer =
        new MongoCollectionInitializer(mock(MongoDatabase.class), mock(MongoClient.class));
    when(collection.createIndex(any(Bson.class), any(IndexOptions.class)))
        .thenThrow(commandFailure(86));
    doThrow(commandFailure(13)).when(collection).dropIndex("idx_job_depends_on");

    assertThrows(MongoCommandException.class, () -> initializer.createDependsOnIndex(collection));
    verify(collection).createIndex(any(Bson.class), any(IndexOptions.class));
  }

  private static MongoCommandException commandFailure(int code) {
    return new MongoCommandException(
        new BsonDocument("code", new BsonInt32(code))
            .append("errmsg", new BsonString("index operation failed")),
        new ServerAddress());
  }
}
