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
package run.ratchet.spring.boot.autoconfigure.mongodb;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** MongoDB connection settings for Ratchet's isolated MongoDB store. */
@ConfigurationProperties(RatchetMongoProperties.PREFIX)
public final class RatchetMongoProperties {

  public static final String PREFIX = "ratchet.mongodb";
  public static final String CONNECTION_STRING_PROPERTY = PREFIX + ".connection-string";
  public static final String DATABASE_PROPERTY = PREFIX + ".database";

  /** MongoDB connection string used to create Ratchet's client. */
  private String connectionString;

  /** MongoDB database containing Ratchet collections. */
  private String database;

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }
}
