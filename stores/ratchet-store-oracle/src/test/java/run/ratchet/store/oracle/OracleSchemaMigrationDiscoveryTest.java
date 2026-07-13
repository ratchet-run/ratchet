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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.JdbcDriverDataSource;

class OracleSchemaMigrationDiscoveryTest {

  @Test
  void preservesV001ChecksumAndContiguousCrossStoreVersions() throws Exception {
    List<SchemaMigrator.MigrationScript> migrations =
        new SchemaMigrator(
                new JdbcDriverDataSource("jdbc:oracle:thin:@unused", "unused", "unused"),
                new OracleSchemaMigrationDialect())
            .discoverMigrations();
    SchemaMigrator.MigrationScript v001 = migrations.get(0);

    assertEquals(
        List.of("001", "002", "003", "004", "005", "006"),
        migrations.stream().map(script -> script.version()).toList());
    assertEquals(
        "2862e471ae72a3e9c6d5182b6d1819d1af9e9055fbac0ee405bbedd0277ef5a2", v001.checksum());
  }
}
