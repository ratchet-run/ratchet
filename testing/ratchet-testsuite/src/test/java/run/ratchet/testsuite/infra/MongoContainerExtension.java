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
package run.ratchet.testsuite.infra;

import java.util.logging.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * JUnit 5 extension that starts a Testcontainers MongoDB instance before all tests.
 *
 * <p>Only activates when {@code ratchet.test.db.type} is set to {@code "mongodb"}. The container is
 * started once and shared across all test classes via the JUnit {@link ExtensionContext.Store} with
 * GLOBAL namespace.
 *
 * <p>System properties are set for the MongoDB connection string and database name, which are read
 * by the in-container {@code TestMongoProducer} CDI bean to provide a {@code MongoDatabase}
 * instance.
 */
public class MongoContainerExtension
    implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private static final Logger log = Logger.getLogger(MongoContainerExtension.class.getName());

  private static final String STORE_KEY = "ratchet-mongo-container";

  private static volatile MongoDBContainer container;
  private static volatile MongoStoreConfig config;
  private static volatile boolean started = false;

  public static MongoStoreConfig getConfig() {
    if (config == null) {
      throw new IllegalStateException(
          "MongoContainerExtension has not been initialized. "
              + "Ensure the mongodb profile is active.");
    }
    return config;
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    if (!"mongodb".equals(dbType)) {
      return;
    }

    if (started) {
      return;
    }

    synchronized (MongoContainerExtension.class) {
      if (started) {
        return;
      }

      context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).put(STORE_KEY, this);

      log.info("Starting Testcontainers MongoDB");

      container = new MongoDBContainer("mongo:7.0").withReplicaSet();
      container.start();

      config = new MongoStoreConfig(container.getConnectionString(), "ratchet_test");

      System.setProperty("ratchet.test.mongo.uri", config.connectionString());
      System.setProperty("ratchet.test.mongo.database", config.databaseName());

      started = true;
      log.info("MongoDB container ready for database " + config.databaseName());
    }
  }

  @Override
  public void close() {
    if (container != null && container.isRunning()) {
      container.stop();
      log.info("MongoDB container stopped");
    }
  }
}
