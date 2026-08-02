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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;

/** Lazily starts the shared MongoDB Testcontainers instance used by Mongo integration tests. */
public final class MongoSharedContainer {

  private static final AtomicBoolean CLOSED = new AtomicBoolean();

  private MongoSharedContainer() {}

  /**
   * Returns the connection string for the shared MongoDB container, starting it on first use.
   *
   * @return the shared container connection string
   */
  public static String connectionString() {
    return Holder.CONTAINER.getConnectionString();
  }

  static void close() {
    if (CLOSED.compareAndSet(false, true)) {
      Holder.CONTAINER.close();
    }
  }

  private static MongoDBContainer startContainer() {
    MongoDBContainer container =
        new MongoDBContainer("mongo:7.0")
            .withReplicaSet()
            .waitingFor(
                Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    container.start();
    return container;
  }

  private static final class Holder {
    private static final MongoDBContainer CONTAINER = startContainer();
  }
}
