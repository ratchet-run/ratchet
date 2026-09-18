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
package run.ratchet.consumer.mongodb;

import java.util.Map;
import org.testcontainers.mongodb.MongoDBContainer;

/** Creates the replica-set MongoDB containers shared by the Spring consumer tests. */
public final class MongoDatabase {
  private static final String TEST_TMPFS = "RATCHET_TEST_TMPFS";
  private static final Map<String, String> TMPFS_MAPPINGS = Map.of("/data/db", "rw");

  private MongoDatabase() {}

  /**
   * Creates a MongoDB replica set and, when requested, keeps its data on a Docker tmpfs.
   *
   * <p>The tmpfs is opt-in because it is useful for constrained Docker overlay filesystems but
   * intentionally leaves normal Testcontainers behavior unchanged.
   */
  public static MongoDBContainer create() {
    MongoDBContainer database = new MongoDBContainer("mongo:7.0").withReplicaSet();
    if ("true".equalsIgnoreCase(System.getenv(TEST_TMPFS))) {
      database.withTmpFs(TMPFS_MAPPINGS);
    }
    return database;
  }
}
