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
package run.ratchet.quarkus.mongodb.runtime;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.quarkus.arc.DefaultBean;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import run.ratchet.store.mongodb.MongoClientFactory;

/** Default MongoDB beans for the Ratchet MongoDB Quarkus flavor. */
@ApplicationScoped
public class QuarkusRatchetMongoProducers {

  public static final String CONNECTION_STRING_PROPERTY = "quarkus.mongodb.connection-string";
  public static final String DATABASE_PROPERTY = "quarkus.mongodb.database";
  public static final String DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017";
  public static final String DEFAULT_DATABASE = "ratchet";

  private MongoClient managedClient;

  /**
   * Fallback client for hosts that do not use Quarkus' own MongoClient bean. Quarkus' client is also
   * safe for Ratchet because {@link QuarkusRatchetMongoClientCustomizer} forces STANDARD UUIDs.
   */
  @Produces
  @ApplicationScoped
  @DefaultBean
  public synchronized MongoClient mongoClient(
      @ConfigProperty(name = CONNECTION_STRING_PROPERTY, defaultValue = DEFAULT_CONNECTION_STRING)
          String connectionString) {
    if (managedClient == null) {
      managedClient = MongoClientFactory.create(connectionString);
    }
    return managedClient;
  }

  @Produces
  @ApplicationScoped
  @DefaultBean
  public MongoDatabase mongoDatabase(
      MongoClient client,
      @ConfigProperty(name = DATABASE_PROPERTY, defaultValue = DEFAULT_DATABASE)
          String databaseName) {
    String effectiveDatabase = databaseName.strip();
    if (effectiveDatabase.isEmpty()) {
      throw new IllegalArgumentException(DATABASE_PROPERTY + " must not be blank");
    }
    return client.getDatabase(effectiveDatabase);
  }

  @PreDestroy
  void closeManagedClient() {
    if (managedClient != null) {
      managedClient.close();
    }
  }
}
