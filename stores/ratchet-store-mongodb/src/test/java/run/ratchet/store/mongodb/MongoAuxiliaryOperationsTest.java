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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static run.ratchet.store.mongodb.MongoFieldNames.ACTIVE_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.NODE_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;

import com.mongodb.MongoClientSettings;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MongoAuxiliaryOperationsTest {

  @Test
  @SuppressWarnings("unchecked")
  void cleanupDeletesOnlyEndedClaimsAndDecrementsOnlyTheirCounts() {
    for (boolean missing : List.of(false, true)) {
      var ctx = mock(MongoStoreContext.class);
      var session = mock(ClientSession.class);
      MongoCollection<Document> permits = mock(MongoCollection.class);
      MongoCollection<Document> jobs = mock(MongoCollection.class);
      MongoCollection<Document> nodes = mock(MongoCollection.class);
      MongoCollection<Document> limits = mock(MongoCollection.class);
      when(ctx.startSession()).thenReturn(session);
      when(ctx.resourcePermits()).thenReturn(permits);
      when(ctx.jobs()).thenReturn(jobs);
      when(ctx.resourceLimits()).thenReturn(limits);
      if (missing) when(ctx.nodes()).thenReturn(nodes);
      when(session.withTransaction(any(TransactionBody.class), any(TransactionOptions.class)))
          .thenAnswer(invocation -> ((TransactionBody<?>) invocation.getArgument(0)).execute());
      List<Document> candidates =
          List.of(
              permit("kept", "live-job", "res-kept"),
              permit("moved", "moved-job", "res-removed"),
              permit("gone", "gone-job", "res-removed"));
      when(permits.find(eq(session), any(Bson.class))).thenReturn(iterable(candidates));
      when(jobs.find(eq(session), any(Bson.class)))
          .thenAnswer(
              invocation -> {
                Document filter = bson(invocation.getArgument(1));
                assertTrue(filter.toJson().contains("RUNNING"));
                assertTrue(filter.toJson().contains("live-job"));
                return iterable(
                    List.of(
                        new Document(ID, "live-job")
                            .append(STATUS, "RUNNING")
                            .append(PICKED_BY, "stale"),
                        new Document(ID, "moved-job")
                            .append(STATUS, "RUNNING")
                            .append(PICKED_BY, "new")));
              });
      if (missing) when(nodes.find(eq(session), any(Bson.class))).thenReturn(iterable(List.of()));
      when(permits.deleteMany(eq(session), any(Bson.class)))
          .thenReturn(DeleteResult.acknowledged(2));
      var operations = new MongoAuxiliaryOperations(ctx);
      assertEquals(
          2,
          missing
              ? operations.cleanupPermitsOfMissingNodes(Instant.now())
              : operations.cleanupOrphanedPermits(List.of("stale")));
      ArgumentCaptor<Bson> deleted = ArgumentCaptor.forClass(Bson.class);
      verify(permits).deleteMany(eq(session), deleted.capture());
      assertEquals(
          new Document(ID, new Document("$in", List.of("moved", "gone"))),
          bson(deleted.getValue()));
      ArgumentCaptor<List<WriteModel<Document>>> writes = ArgumentCaptor.forClass(List.class);
      verify(limits).bulkWrite(eq(session), writes.capture(), any(BulkWriteOptions.class));
      assertEquals(1, writes.getValue().size());
      var update = (UpdateOneModel<Document>) writes.getValue().get(0);
      assertEquals(new Document(ID, "res-removed"), bson(update.getFilter()));
      assertEquals(new Document("$inc", new Document(ACTIVE_COUNT, -2)), bson(update.getUpdate()));
    }
  }

  private static Document permit(String id, String job, String resource) {
    return new Document(ID, id)
        .append(JOB_ID, job)
        .append(NODE_ID, "stale")
        .append(RESOURCE_NAME, resource);
  }

  @SuppressWarnings("unchecked")
  private static FindIterable<Document> iterable(List<Document> documents) {
    FindIterable<Document> iterable = mock(FindIterable.class);
    when(iterable.into(any(ArrayList.class)))
        .thenAnswer(
            invocation -> {
              ArrayList<Document> result = invocation.getArgument(0);
              result.addAll(documents);
              return result;
            });
    doAnswer(
            invocation -> {
              Consumer<Document> consumer = invocation.getArgument(0);
              documents.forEach(consumer);
              return null;
            })
        .when(iterable)
        .forEach(any(Consumer.class));
    return iterable;
  }

  private static Document bson(Bson value) {
    return Document.parse(
        value
            .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry())
            .toJson());
  }
}
