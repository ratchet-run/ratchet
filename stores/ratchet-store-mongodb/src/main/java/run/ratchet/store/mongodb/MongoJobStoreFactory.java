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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.Objects;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;

/** Creates ready-to-use MongoDB job stores without requiring CDI. */
public final class MongoJobStoreFactory {

  private MongoJobStoreFactory() {}

  /**
   * Creates and fully initializes a MongoDB job store.
   *
   * <p>The caller owns the supplied client and remains responsible for closing it.
   *
   * @param client MongoDB client used by the store
   * @param database MongoDB database used by the store
   * @param options Ratchet runtime options
   * @param metricsCollector collector for store metrics
   * @return an initialized MongoDB job store
   */
  public static MongoJobStore create(
      MongoClient client,
      MongoDatabase database,
      RatchetOptions options,
      MetricsCollector metricsCollector) {
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(options, "options");
    Objects.requireNonNull(metricsCollector, "metricsCollector");

    MongoJobStoreImpl store = new MongoJobStoreImpl(client, database, options, metricsCollector);
    store.initializeCollections();
    return store;
  }

  /**
   * Creates and fully initializes a MongoDB job store using a no-op metrics collector.
   *
   * <p>The caller owns the supplied client and remains responsible for closing it.
   *
   * @param client MongoDB client used by the store
   * @param database MongoDB database used by the store
   * @param options Ratchet runtime options
   * @return an initialized MongoDB job store
   */
  public static MongoJobStore create(
      MongoClient client, MongoDatabase database, RatchetOptions options) {
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(options, "options");

    MongoJobStoreImpl store = new MongoJobStoreImpl(client, database, options);
    store.initializeCollections();
    return store;
  }
}
