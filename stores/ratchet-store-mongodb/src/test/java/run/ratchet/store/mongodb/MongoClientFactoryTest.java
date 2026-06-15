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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MongoClientFactoryTest {

  @Test
  void createRejectsNullConnectionStringAtFactoryBoundary() {
    NullPointerException ex =
        assertThrows(NullPointerException.class, () -> MongoClientFactory.create(null));

    assertTrue(ex.getMessage().contains("connectionString"));
  }

  @Test
  void createRejectsBlankConnectionStringAtFactoryBoundary() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> MongoClientFactory.create(" \t "));

    assertTrue(ex.getMessage().contains("connectionString"));
  }
}
