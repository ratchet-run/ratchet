package run.ratchet.tck.store;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Pool-less {@link DataSource} that delegates every {@code getConnection} call to {@link
 * DriverManager}. Intended for tests and low-frequency tooling (schema migration, conformance
 * fixtures); never use in production paths.
 */
public final class JdbcDriverDataSource implements DataSource {

  private final String url;
  private final String user;
  private final String password;

  public JdbcDriverDataSource(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }

  @Override
  public Connection getConnection(String u, String p) throws SQLException {
    return DriverManager.getConnection(url, u, p);
  }

  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) {}

  @Override
  public void setLoginTimeout(int seconds) {}

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger("global");
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }
}
