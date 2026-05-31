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
package run.ratchet.testsuite.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import run.ratchet.store.mongodb.MongoClientFactory;

/** Produces MongoDB test handles from MongoContainerExtension system properties. */
@ApplicationScoped
public class TestMongoProducer {

  private volatile MongoClient client;

  @Produces
  @ApplicationScoped
  public MongoClient mongoClient() {
    String uri = TestRuntimeConfig.mongoUri();
    if (uri == null || uri.isBlank()) {
      throw new IllegalStateException(
          "ratchet.test.mongo.uri system property not set. "
              + "Ensure MongoContainerExtension is active and the mongodb profile is enabled.");
    }
    client = MongoClientFactory.create(uri);
    return client;
  }

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase(MongoClient mongoClient) {
    String dbName = TestRuntimeConfig.mongoDatabase();
    return mongoClient.getDatabase(dbName);
  }

  @PreDestroy
  void cleanup() {
    if (client != null) {
      client.close();
    }
  }
}
