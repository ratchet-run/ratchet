package run.ratchet.testsuite.util;

import run.ratchet.testsuite.infra.JdbcDatabaseConfig;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * WildFly-specific datasource configuration strategy.
 *
 * <p>The datasource and JDBC driver are configured in standalone.xml via system property
 * expressions. The JDBC driver is installed as a WildFly module by the Maven build. This class is a
 * no-op because the datasource is managed at the server level, not within the deployment.
 */
public class WildflyDataSourceStrategy implements DataSourceStrategy {

  @Override
  public void configureArchive(WebArchive archive, JdbcDatabaseConfig config) {
    // No-op: datasource is configured in standalone.xml with system property expressions.
    // The JDBC driver module and datasource are installed by the Maven build
    // (see wildfly-managed profile in pom.xml).
  }
}
