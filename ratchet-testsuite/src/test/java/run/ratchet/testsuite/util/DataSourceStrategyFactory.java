package run.ratchet.testsuite.util;

public final class DataSourceStrategyFactory {

  private DataSourceStrategyFactory() {}

  /** Reads {@code arquillian.launch} to pick the right strategy. */
  public static DataSourceStrategy create() {
    String launch = System.getProperty("arquillian.launch", "wildfly-managed");
    return switch (launch) {
      case "wildfly-managed", "wildfly-ee11-managed" -> new WildflyDataSourceStrategy();
      case "payara-managed" -> new PayaraDataSourceStrategy();
      case "glassfish-managed" -> new GlassFishDataSourceStrategy();
      case "openliberty-managed" -> new OpenLibertyDataSourceStrategy();
      default -> throw new IllegalArgumentException("No DataSourceStrategy for server: " + launch);
    };
  }
}
