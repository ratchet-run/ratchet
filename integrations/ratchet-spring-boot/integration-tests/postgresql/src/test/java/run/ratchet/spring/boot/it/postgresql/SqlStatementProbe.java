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
package run.ratchet.spring.boot.it.postgresql;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Records SQL that reaches JDBC statement execution and can inject a selected execution failure.
 */
public final class SqlStatementProbe {

  private final List<String> statements = new CopyOnWriteArrayList<>();
  private final Predicate<String> failWhen;

  public SqlStatementProbe() {
    this(sql -> false);
  }

  public SqlStatementProbe(Predicate<String> failWhen) {
    this.failWhen = failWhen;
  }

  public DataSource wrap(DataSource delegate) {
    return new ProbedDataSource(delegate);
  }

  public long countContaining(String fragment) {
    String normalizedFragment = normalize(fragment);
    return statements.stream().filter(sql -> sql.contains(normalizedFragment)).count();
  }

  public int firstIndexContaining(String fragment) {
    String normalizedFragment = normalize(fragment);
    for (int index = 0; index < statements.size(); index++) {
      if (statements.get(index).contains(normalizedFragment)) {
        return index;
      }
    }
    return -1;
  }

  public int lastIndexContaining(String fragment) {
    String normalizedFragment = normalize(fragment);
    for (int index = statements.size() - 1; index >= 0; index--) {
      if (statements.get(index).contains(normalizedFragment)) {
        return index;
      }
    }
    return -1;
  }

  public List<String> statements() {
    return List.copyOf(statements);
  }

  private void beforeExecution(String sql) throws SQLException {
    if (sql == null) {
      return;
    }
    String normalized = normalize(sql);
    statements.add(normalized);
    if (failWhen.test(normalized)) {
      throw new SQLException("Intentional PostgreSQL migration failure for integration testing");
    }
  }

  private Connection connectionProxy(Connection delegate) {
    return (Connection)
        Proxy.newProxyInstance(
            delegate.getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> invokeConnection(delegate, proxy, method, args));
  }

  private Object invokeConnection(
      Connection delegate, Object proxy, Method method, Object[] arguments) throws Throwable {
    if (method.getDeclaringClass() == Object.class) {
      return invokeObjectMethod(delegate, proxy, method, arguments);
    }

    Object result = invoke(delegate, method, arguments);
    if (!(result instanceof Statement statement)) {
      return result;
    }

    String preparedSql =
        arguments != null
                && arguments.length > 0
                && arguments[0] instanceof String sql
                && (method.getName().startsWith("prepare"))
            ? sql
            : null;
    return statementProxy(statement, preparedSql);
  }

  private Statement statementProxy(Statement delegate, String preparedSql) {
    Class<?> statementType =
        delegate instanceof CallableStatement
            ? CallableStatement.class
            : delegate instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
    return (Statement)
        Proxy.newProxyInstance(
            delegate.getClass().getClassLoader(),
            new Class<?>[] {statementType},
            (proxy, method, args) -> invokeStatement(delegate, proxy, method, args, preparedSql));
  }

  private Object invokeStatement(
      Statement delegate, Object proxy, Method method, Object[] arguments, String preparedSql)
      throws Throwable {
    if (method.getDeclaringClass() == Object.class) {
      return invokeObjectMethod(delegate, proxy, method, arguments);
    }

    if (method.getName().startsWith("execute")) {
      String sql =
          arguments != null && arguments.length > 0 && arguments[0] instanceof String suppliedSql
              ? suppliedSql
              : preparedSql;
      beforeExecution(sql);
    }
    return invoke(delegate, method, arguments);
  }

  private static Object invoke(Object delegate, Method method, Object[] arguments)
      throws Throwable {
    try {
      return method.invoke(delegate, arguments);
    } catch (InvocationTargetException exception) {
      throw exception.getCause();
    }
  }

  private static Object invokeObjectMethod(
      Object delegate, Object proxy, Method method, Object[] arguments) {
    return switch (method.getName()) {
      case "equals" -> proxy == arguments[0];
      case "hashCode" -> System.identityHashCode(proxy);
      case "toString" -> "SqlStatementProbe[" + delegate + "]";
      default -> throw new IllegalStateException("Unexpected Object method " + method.getName());
    };
  }

  private static String normalize(String sql) {
    return sql.trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
  }

  private final class ProbedDataSource implements DataSource {

    private final DataSource delegate;

    private ProbedDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return connectionProxy(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return connectionProxy(delegate.getConnection(username, password));
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      if (type.isInstance(this)) {
        return type.cast(this);
      }
      return delegate.unwrap(type);
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
      return type.isInstance(this) || delegate.isWrapperFor(type);
    }
  }
}
