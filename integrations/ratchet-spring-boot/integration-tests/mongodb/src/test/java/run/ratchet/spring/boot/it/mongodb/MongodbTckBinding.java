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
package run.ratchet.spring.boot.it.mongodb;

import com.mongodb.client.MongoDatabase;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.annotation.Autowired;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.StoreTckBinding;
import run.ratchet.store.mongodb.MongoSharedContainer;

/** MongoDB-specific behavior for the shared Spring TCK fixtures. */
public final class MongodbTckBinding implements StoreTckBinding {

  private static final String DATABASE_NAME =
      "ratchet_spring_tck_" + UUID.randomUUID().toString().replace("-", "");

  private MongoDatabase database;

  @Override
  public void beforeAll(ExtensionContext context) {}

  @Override
  public String[] mainContextProperties() {
    return new String[] {
      "ratchet.mongodb.connection-string=" + MongoSharedContainer.connectionString(),
      "ratchet.mongodb.database=" + DATABASE_NAME,
      "ratchet.class-policy.allowed-packages=" + allowedPackagesProperty(),
      "ratchet.lifecycle.drain-timeout=PT30S"
    };
  }

  @Override
  public String[] clockedContextProperties() {
    return new String[] {
      "ratchet.class-policy.allowed-packages=" + allowedPackagesProperty(),
      "ratchet.lifecycle.drain-timeout=PT30S",
      "spring.autoconfigure.exclude="
          + "run.ratchet.spring.boot.autoconfigure.mongodb.RatchetMongoAutoConfiguration,"
          + "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration"
    };
  }

  @Override
  public String applicationPackage() {
    return "run.ratchet.spring.boot.it.mongodb";
  }

  @Autowired
  void setDatabase(MongoDatabase database) {
    this.database = database;
  }

  @Override
  public void clearStore() {
    for (String collectionName : database.listCollectionNames()) {
      database.getCollection(collectionName).deleteMany(new Document());
    }
  }

  @Override
  public boolean supportsCallerTransactionRollback() {
    return false;
  }

  @Override
  public String runtimeName() {
    return "SpringMongoRatchetTckRuntime";
  }

  @Override
  public String clockedRuntimeName() {
    return "SpringMongoClockedTckRuntime";
  }
}
