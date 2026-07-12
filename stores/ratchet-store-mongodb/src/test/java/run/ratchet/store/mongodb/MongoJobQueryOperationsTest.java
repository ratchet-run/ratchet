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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.MongoClientSettings;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;

class MongoJobQueryOperationsTest {

  @Test
  void archiveSearchRejectsDeepOffsetWithoutCursor() {
    MongoJobQueryOperations operations = new MongoJobQueryOperations(null);
    JobFilter filter = JobFilter.builder().includeArchived(true).build();

    assertThrows(IllegalArgumentException.class, () -> operations.searchJobs(filter, 1000, 1001));
  }

  @Test
  void priorityFilterUsesStablePersistedCode() {
    JobFilter filter = JobFilter.builder().priorities(JobPriority.HIGH).build();
    List<Bson> conditions = new ArrayList<>();

    MongoJobQueryOperations.appendPriorityCondition(filter, conditions);

    BsonDocument condition =
        conditions
            .get(0)
            .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
    assertEquals(
        JobPriority.HIGH.persistedCode(),
        condition.getDocument("priority").getArray("$in").get(0).asInt32().getValue());
  }
}
