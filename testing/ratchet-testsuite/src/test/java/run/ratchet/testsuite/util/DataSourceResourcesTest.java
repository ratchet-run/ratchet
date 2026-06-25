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
package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class DataSourceResourcesTest {

  @Test
  void xmlEscapesAttributeSensitiveCharacters() {
    assertEquals(
        "jdbc:mysql://host/db?x=1&amp;y=&quot;two&quot;&lt;tag&gt;",
        DataSourceResources.xml("jdbc:mysql://host/db?x=1&y=\"two\"<tag>"));
  }

  @Test
  void dataSourceClassNamesMatchSupportedDatabases() {
    assertEquals(
        "com.mysql.cj.jdbc.MysqlDataSource", DataSourceResources.dataSourceClassName("mysql"));
    assertEquals(
        "org.postgresql.ds.PGSimpleDataSource",
        DataSourceResources.dataSourceClassName("postgresql"));
    assertEquals(
        "oracle.jdbc.pool.OracleDataSource", DataSourceResources.dataSourceClassName("oracle"));
  }

  @Test
  void driverCoordinatesMatchSupportedDatabases() {
    assertEquals("com.mysql:mysql-connector-j", DataSourceResources.driverCoordinates("mysql"));
    assertEquals("org.postgresql:postgresql", DataSourceResources.driverCoordinates("postgresql"));
    assertEquals(
        "com.oracle.database.jdbc:ojdbc11", DataSourceResources.driverCoordinates("oracle"));
  }

  @Test
  void unsupportedDatabaseTypeIsRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> DataSourceResources.dataSourceClassName("sqlserver"));
    assertThrows(
        IllegalArgumentException.class, () -> DataSourceResources.driverCoordinates("sqlserver"));
  }
}
