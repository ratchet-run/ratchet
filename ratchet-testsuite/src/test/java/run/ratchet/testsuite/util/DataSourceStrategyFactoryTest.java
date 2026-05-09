package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DataSourceStrategyFactoryTest {

  private static final String LAUNCH_PROPERTY = "arquillian.launch";

  @AfterEach
  void clearLaunchProperty() {
    System.clearProperty(LAUNCH_PROPERTY);
  }

  @Test
  void defaultsToWildflyWhenLaunchPropertyIsUnset() {
    System.clearProperty(LAUNCH_PROPERTY);

    DataSourceStrategy strategy = DataSourceStrategyFactory.create();

    assertInstanceOf(WildflyDataSourceStrategy.class, strategy);
    assertEquals("java:jboss/datasources/RatchetDS", strategy.jtaDataSourceName());
  }

  @Test
  void mapsKnownManagedServersToStrategies() {
    assertStrategy("wildfly-managed", WildflyDataSourceStrategy.class);
    assertStrategy("payara-managed", PayaraDataSourceStrategy.class);
    assertStrategy("glassfish-managed", GlassFishDataSourceStrategy.class);
    assertStrategy("openliberty-managed", OpenLibertyDataSourceStrategy.class);
  }

  @Test
  void rejectsUnknownLaunchProperty() {
    System.setProperty(LAUNCH_PROPERTY, "unknown-managed");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, DataSourceStrategyFactory::create);

    assertEquals("No DataSourceStrategy for server: unknown-managed", exception.getMessage());
  }

  private static void assertStrategy(
      String launch, Class<? extends DataSourceStrategy> expectedType) {
    System.setProperty(LAUNCH_PROPERTY, launch);

    assertInstanceOf(expectedType, DataSourceStrategyFactory.create());
  }
}
