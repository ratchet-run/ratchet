package run.ratchet.testsuite.util;

/**
 * Selects the appropriate {@link DataSourceStrategy} based on the active application server
 * profile.
 */
public final class DataSourceStrategyFactory {

  private DataSourceStrategyFactory() {}

  /**
   * Returns the datasource strategy for the current application server.
   *
   * <p>The server is determined by the {@code arquillian.launch} system property.
   *
   * @return the appropriate datasource strategy
   */
  public static DataSourceStrategy create() {
    String launch = System.getProperty("arquillian.launch", "wildfly-managed");
    if (launch == null || launch.isBlank()) {
      launch = "wildfly-managed";
    }
    return switch (launch) {
      case "wildfly-managed" -> new WildflyDataSourceStrategy();
      default -> throw new IllegalArgumentException("No DataSourceStrategy for server: " + launch);
    };
  }
}
