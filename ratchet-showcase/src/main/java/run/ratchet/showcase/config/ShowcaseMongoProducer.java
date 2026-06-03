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
package run.ratchet.showcase.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class ShowcaseMongoProducer {

  private MongoClient client;

  @Produces
  @ApplicationScoped
  public MongoClient mongoClient() {
    String uri = envOrDefault("MONGO_URI", "mongodb://localhost:27017");
    client = uuidSafeClient(uri);
    return client;
  }

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase(MongoClient mongoClient) {
    return mongoClient.getDatabase(envOrDefault("MONGO_DATABASE", "ratchet"));
  }

  @PreDestroy
  void close() {
    if (client != null) {
      client.close();
    }
  }

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static MongoClient uuidSafeClient(String uri) {
    try {
      Class<?> factory = Class.forName("run.ratchet.store.mongodb.MongoClientFactory");
      return (MongoClient) factory.getMethod("create", String.class).invoke(null, uri);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "MongoDB showcase profile requires ratchet-store-mongodb on the WAR classpath", e);
    }
  }
}
