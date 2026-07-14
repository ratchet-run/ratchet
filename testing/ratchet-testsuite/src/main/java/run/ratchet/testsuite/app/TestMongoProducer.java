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
import java.util.Arrays;
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

    // Bound how long a monitor thread blocked in connect/heartbeat I/O can survive close(), keeping
    // its post-close lifetime under the disposer join budget so the WAR classloader never closes
    // while a driver thread is still alive.
    String query = uri.contains("?") ? uri.substring(uri.indexOf('?') + 1) : "";
    List<String> queryParameterNames =
        Arrays.stream(query.split("&")).map(parameter -> parameter.split("=", 2)[0]).toList();
    if (queryParameterNames.stream()
        .noneMatch(parameter -> parameter.equalsIgnoreCase("heartbeatFrequencyMS"))) {
      uri += (uri.contains("?") ? "&" : "?") + "heartbeatFrequencyMS=2000";
    }
    if (queryParameterNames.stream()
        .noneMatch(parameter -> parameter.equalsIgnoreCase("connectTimeoutMS"))) {
      uri += (uri.contains("?") ? "&" : "?") + "connectTimeoutMS=2000";
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
    // MongoDB close() signals monitor threads, but one blocked in socket I/O dies only when the
    // now-bounded timeout fires. Join to reap it before GlassFish closes the WAR classloader.
    // Interrupt halfway through covers interruptible waits but cannot break a blocking socket read.
    long startNanos = System.nanoTime();
    long interruptDeadlineNanos = startNanos + MONITOR_SHUTDOWN_TIMEOUT_NANOS / 2;
    long deadlineNanos = startNanos + MONITOR_SHUTDOWN_TIMEOUT_NANOS;
    List<Thread> monitorThreads =
        Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .filter(thread -> thread.getName().startsWith("cluster-"))
            .toList();

    boolean waitInterrupted = false;
    for (int phase = 0; phase < 2 && !waitInterrupted; phase++) {
      long phaseDeadlineNanos = phase == 0 ? interruptDeadlineNanos : deadlineNanos;
      if (phase == 1) {
        monitorThreads.stream().filter(Thread::isAlive).forEach(Thread::interrupt);
      }

      for (Thread thread : monitorThreads) {
        if (!thread.isAlive()) {
          continue;
        }
        long remainingNanos = phaseDeadlineNanos - System.nanoTime();
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
          waitInterrupted = true;
          break;
        }
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
