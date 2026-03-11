package run.ratchet.testsuite.util;

import run.ratchet.testsuite.infra.DatabaseConfig;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * Strategy for configuring a datasource within a test deployment archive.
 *
 * <p>Each application server has a different mechanism for defining datasources within a deployment
 * (e.g., WildFly uses {@code -ds.xml}, GlassFish uses {@code glassfish-resources.xml}).
 */
public interface DataSourceStrategy {

  /**
   * Configures the given archive with a datasource pointing to the test database.
   *
   * @param archive the web archive to configure
   * @param config the database connection details
   */
  void configureArchive(WebArchive archive, DatabaseConfig config);
}
