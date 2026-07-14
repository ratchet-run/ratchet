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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import run.ratchet.store.mongodb.MongoClientFactory;

/**
 * Produces MongoDB test handles from MongoContainerExtension system properties.
 *
 * <p>The client disposer waits for the driver's background monitor threads to exit before returning
 * so GlassFish can safely close the WAR classloader during undeploy.
 */
@ApplicationScoped
public class TestMongoProducer {

  private static final Logger log = Logger.getLogger(TestMongoProducer.class.getName());
  private static final long MONITOR_SHUTDOWN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);

  @Produces
  @ApplicationScoped
  public MongoClient mongoClient() {
    String uri = TestRuntimeConfig.mongoUri();
    if (uri == null || uri.isBlank()) {
      throw new IllegalStateException(
          "ratchet.test.mongo.uri system property not set. "
              + "Ensure MongoContainerExtension is active and the mongodb profile is enabled.");
    }
    return MongoClientFactory.create(uri);
  }

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase(MongoClient mongoClient) {
    String dbName = TestRuntimeConfig.mongoDatabase();
    return mongoClient.getDatabase(dbName);
  }

  void closeClient(@Disposes MongoClient client) {
    try {
      client.close();
    } catch (RuntimeException e) {
      log.warning("Failed to close MongoDB test client: " + e.getMessage());
    }

    try {
      waitForMonitorThreads();
    } catch (RuntimeException e) {
      log.warning("Failed while waiting for MongoDB monitor threads to stop: " + e.getMessage());
    }
  }

  private static void waitForMonitorThreads() {
    // GlassFish closes the WebappClassLoader at undeploy and hard-fails later class loads. MongoDB
    // monitor threads lazily load classes during shutdown, so wait for them before undeploy
    // returns.
    long deadlineNanos = System.nanoTime() + MONITOR_SHUTDOWN_TIMEOUT_NANOS;
    List<Thread> monitorThreads =
        Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .filter(thread -> thread.getName().startsWith("cluster-"))
            .toList();

    for (Thread thread : monitorThreads) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      try {
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        int remainingNanosPart =
            (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(remainingMillis));
        thread.join(remainingMillis, remainingNanosPart);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    List<String> remainingMonitorNames =
        monitorThreads.stream().filter(Thread::isAlive).map(Thread::getName).sorted().toList();
    if (!remainingMonitorNames.isEmpty()) {
      log.warning(
          "MongoDB monitor threads still alive after shutdown wait: "
              + String.join(", ", remainingMonitorNames));
    }
  }
}
