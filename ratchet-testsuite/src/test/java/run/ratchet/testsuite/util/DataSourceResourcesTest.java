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
  }

  @Test
  void driverCoordinatesMatchSupportedDatabases() {
    assertEquals("com.mysql:mysql-connector-j", DataSourceResources.driverCoordinates("mysql"));
    assertEquals("org.postgresql:postgresql", DataSourceResources.driverCoordinates("postgresql"));
  }

  @Test
  void unsupportedDatabaseTypeIsRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> DataSourceResources.dataSourceClassName("oracle"));
    assertThrows(
        IllegalArgumentException.class, () -> DataSourceResources.driverCoordinates("oracle"));
  }
}
