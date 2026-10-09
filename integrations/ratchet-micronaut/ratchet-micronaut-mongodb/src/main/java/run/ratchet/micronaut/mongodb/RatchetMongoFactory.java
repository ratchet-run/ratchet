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
package run.ratchet.micronaut.mongodb;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import jakarta.inject.Singleton;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.mongodb.MongoJobStoreFactory;
import run.ratchet.store.spi.JobStore;

/** Micronaut MongoDB store wiring around the application's Mongo client. */
@Factory
@Requires(beans = MongoClient.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
public class RatchetMongoFactory {

  @Singleton
  @Requires(missingBeans = JobStore.class)
  JobStore ratchetMongoJobStore(
      MongoClient client,
      Environment environment,
      RatchetOptions options,
      MetricsCollector metricsCollector,
      RuntimeContextInstallation installation) {
    // Resolving installation first installs the serializer and encryption context before migration.
    return MongoJobStoreFactory.create(
        client.getDatabase(databaseName(environment)),
        () -> client.startSession(ClientSessionOptions.builder().build()),
        options,
        metricsCollector,
        options.schema().autoMigrate());
  }

  static String databaseName(Environment environment) {
    var explicit =
        environment
            .getProperty("ratchet.mongodb.database", String.class)
            .filter(value -> !value.isBlank());
    if (explicit.isPresent()) {
      return explicit.get();
    }
    var database =
        environment
            .getProperty("mongodb.uri", String.class)
            .filter(value -> !value.isBlank())
            .map(uri -> new ConnectionString(uri).getDatabase())
            .filter(value -> !value.isBlank());
    return database.orElseThrow(
        () ->
            new IllegalStateException(
                "Ratchet MongoDB requires a database name: set ratchet.mongodb.database or"
                    + " include a database in mongodb.uri (mongodb://host:27017/ratchet)."));
  }
}
