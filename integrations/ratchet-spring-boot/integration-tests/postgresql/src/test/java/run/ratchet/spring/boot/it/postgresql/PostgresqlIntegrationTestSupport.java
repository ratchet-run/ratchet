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

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.persistence.EntityManagerFactory;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.orm.jpa.JpaTransactionManager;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.postgresql.PostgresqlJobStore;

@ExtendWith(PostgresqlContainerExtension.class)
abstract class PostgresqlIntegrationTestSupport {

  @BeforeEach
  void resetDatabase() throws SQLException {
    executeSql("DROP SCHEMA public CASCADE", "CREATE SCHEMA public");
  }

  final ApplicationContextRunner contextRunner(
      Class<?> application, RatchetOptions options, Class<?>... additionalConfigurations) {
    Class<?>[] configurations = new Class<?>[additionalConfigurations.length + 1];
    configurations[0] = application;
    System.arraycopy(
        additionalConfigurations, 0, configurations, 1, additionalConfigurations.length);

    return new ApplicationContextRunner()
        .withUserConfiguration(configurations)
        .withInitializer(
            context ->
                ((GenericApplicationContext) context)
                    .registerBean(RatchetOptions.class, () -> options))
        .withPropertyValues(
            "spring.jpa.hibernate.ddl-auto=none",
            "spring.jpa.open-in-view=false",
            "spring.jpa.show-sql=false",
            "logging.level.org.hibernate=WARN");
  }

  static RatchetOptions migrationOptions(String dialect) {
    return RatchetOptions.builder()
        .schema(schema -> schema.autoMigrate(true).migrationDialect(dialect))
        .build();
  }

  static RatchetOptions noMigrationOptions() {
    return RatchetOptions.defaults();
  }

  static JobEntity newPendingJob(String idempotencyKey) {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setIdempotencyKey(idempotencyKey);
    job.setPayload(new JobPayload("example.ConsumerJob", "run", "()V", false, List.of()));
    return job;
  }

  static void assertSingleJpaTopology(ApplicationContext context) {
    assertThat(context.getBeansOfType(EntityManagerFactory.class)).hasSize(1);
    assertThat(context.getBeansOfType(JpaTransactionManager.class)).hasSize(1);
    EntityManagerFactory entityManagerFactory = context.getBean(EntityManagerFactory.class);
    JpaTransactionManager transactionManager = context.getBean(JpaTransactionManager.class);
    assertThat(transactionManager.getEntityManagerFactory()).isSameAs(entityManagerFactory);
  }

  static void assertStoreCoreOrmXmlComesFromInstalledJar() throws Exception {
    Enumeration<URL> discovered =
        Thread.currentThread().getContextClassLoader().getResources("META-INF/orm.xml");
    List<URL> resources = Collections.list(discovered);

    assertThat(resources).hasSize(1);
    URL resource = resources.get(0);
    URL storeCoreLocation = JobEntity.class.getProtectionDomain().getCodeSource().getLocation();
    assertThat(storeCoreLocation.getProtocol()).isEqualTo("file");
    assertThat(storeCoreLocation.getPath()).endsWith(".jar");
    assertThat(storeCoreLocation.getPath()).contains("ratchet-store-core-");
    assertThat(resource.getProtocol()).isEqualTo("jar");
    assertThat(resource.toExternalForm())
        .isEqualTo("jar:" + storeCoreLocation.toExternalForm() + "!/META-INF/orm.xml");
  }

  static PostgresqlJobStore store(ApplicationContext context) {
    return context.getBean(PostgresqlJobStore.class);
  }

  static JpaTransactionManager transactionManager(ApplicationContext context) {
    return context.getBean(JpaTransactionManager.class);
  }

  static EntityManagerFactory entityManagerFactory(ApplicationContext context) {
    return context.getBean(EntityManagerFactory.class);
  }

  static long queryForLong(String sql) throws SQLException {
    try (Connection connection = rawConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      if (!resultSet.next()) {
        throw new IllegalStateException("Query returned no rows: " + sql);
      }
      return resultSet.getLong(1);
    }
  }

  static List<String> queryForStrings(String sql) throws SQLException {
    try (Connection connection = rawConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      List<String> values = new ArrayList<>();
      while (resultSet.next()) {
        values.add(resultSet.getString(1));
      }
      return values;
    }
  }

  static boolean tableExists(String tableName) throws SQLException {
    String escaped = tableName.replace("'", "''");
    return queryForLong(
            "SELECT COUNT(*) FROM information_schema.tables"
                + " WHERE table_schema = 'public' AND table_name = '"
                + escaped
                + "'")
        == 1L;
  }

  static void executeSql(String... statements) throws SQLException {
    try (Connection connection = rawConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        statement.execute(sql);
      }
    }
  }

  static String failureMessages(Throwable failure) {
    List<String> messages = new ArrayList<>();
    Throwable current = failure;
    while (current != null) {
      if (current.getMessage() != null) {
        messages.add(current.getMessage());
      }
      current = current.getCause();
    }
    return String.join("\n", messages);
  }

  static EntityManagerFactory entityManagerFactoryProxy() {
    return (EntityManagerFactory)
        java.lang.reflect.Proxy.newProxyInstance(
            EntityManagerFactory.class.getClassLoader(),
            new Class<?>[] {EntityManagerFactory.class},
            (proxy, method, arguments) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "equals" -> proxy == arguments[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "toString" -> "extraEntityManagerFactory";
                  default -> throw new IllegalStateException(method.getName());
                };
              }
              return defaultValue(method.getReturnType());
            });
  }

  private static Object defaultValue(Class<?> type) {
    if (!type.isPrimitive()) {
      return null;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == char.class) {
      return '\0';
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == float.class) {
      return 0.0F;
    }
    if (type == double.class) {
      return 0.0D;
    }
    return null;
  }

  private static Connection rawConnection() throws SQLException {
    return DriverManager.getConnection(
        PostgresqlContainerExtension.jdbcUrl(),
        PostgresqlContainerExtension.username(),
        PostgresqlContainerExtension.password());
  }
}
