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
package run.ratchet.spring.boot.it.mongodb.fixture.tck;

import com.mongodb.client.MongoDatabase;
import java.util.Objects;
import org.bson.Document;

/** Deletes all scheduler data from the MongoDB TCK database. */
public final class MongoTckStoreCleaner {

  private final MongoDatabase database;

  public MongoTckStoreCleaner(MongoDatabase database) {
    this.database = Objects.requireNonNull(database, "database");
  }

  public void clearAll() {
    for (String collectionName : database.listCollectionNames()) {
      database.getCollection(collectionName).deleteMany(new Document());
    }
  }
}
