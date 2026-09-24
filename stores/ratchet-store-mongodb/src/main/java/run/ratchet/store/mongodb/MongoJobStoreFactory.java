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

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import java.util.Objects;
import java.util.function.Supplier;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;

/** Creates a MongoDB store around application-owned database and session resources. */
public final class MongoJobStoreFactory {

  private MongoJobStoreFactory() {}

  /**
   * Creates and initializes a MongoDB store without creating a Mongo client.
   *
   * <p>The session supplier must open sessions for the same deployment as {@code database}. It is
   * used only for Ratchet's multi-document transactions; collection access and UUID codec
   * validation use the supplied database. The returned composite advertises the mandatory {@code
   * JobStore} contract and every MongoDB optional store capability.
   */
  public static MongoJobStore create(
      MongoDatabase database,
      Supplier<ClientSession> sessions,
      RatchetOptions options,
      MetricsCollector metricsCollector) {
    return create(database, sessions, options, metricsCollector, true);
  }

  /**
   * Creates a store with an explicit collection startup mode for framework integrations.
   *
   * <p>Direct and CDI store construction retains the historic create-and-backfill behavior. An
   * integration that exposes validation-only startup passes {@code false} explicitly.
   */
  public static MongoJobStore create(
      MongoDatabase database,
      Supplier<ClientSession> sessions,
      RatchetOptions options,
      MetricsCollector metricsCollector,
      boolean autoMigrate) {
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(sessions, "sessions");
    Objects.requireNonNull(options, "options");
    Objects.requireNonNull(metricsCollector, "metricsCollector");
    MongoJobStoreImpl store = new MongoJobStoreImpl(database, sessions, options, metricsCollector);
    store.initializeCollections(autoMigrate);
    return store;
  }
}
