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
package run.ratchet.spring.boot.autoconfigure.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import example.ratchet.ConsumerApplication;
import example.ratchet.ConsumerService;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.PayloadSerializerHolder;

/** Proves vendor prerequisites fail before Ratchet starts workers or writes a node heartbeat. */
class SqlPrerequisitesTest {

  @Test
  void selectedVendorRejectsInvalidPrerequisitesBeforeStartingRatchet() {
    try (SqlDatabase database = SqlDatabase.start()) {
      PayloadSerializer baselineSerializer = PayloadSerializerHolder.get();
      List<String> baselineThreads = ratchetThreads();
      String diagnostic = prerequisiteDiagnostic(database.store());

      if (diagnostic != null) {
        assertThatThrownBy(
                () -> {
                  try (ConfigurableApplicationContext ignored =
                      application(database, invalidPrerequisite(database.store()))) {
                    throw new AssertionError(
                        "Consumer unexpectedly started with an invalid prerequisite");
                  }
                })
            .hasStackTraceContaining(diagnostic);

        assertThat(PayloadSerializerHolder.get()).isSameAs(baselineSerializer);
        assertNoHeartbeat(database);
        assertThat(ratchetThreads()).containsExactlyElementsOf(baselineThreads);
      }

      try (ConfigurableApplicationContext recovered =
          application(
              database,
              database.store().equals("mysql")
                  ? new String[] {
                    "--spring.datasource.hikari.transaction-isolation=TRANSACTION_REPEATABLE_READ"
                  }
                  : new String[0])) {
        assertThat(heartbeatCount(database)).isPositive();
        assertJobExecution(recovered, "prerequisite-recovery-" + UUID.randomUUID());
      }

      await()
          .atMost(Duration.ofSeconds(12))
          .untilAsserted(
              () -> {
                assertThat(PayloadSerializerHolder.get()).isSameAs(baselineSerializer);
                assertThat(ratchetThreads()).containsExactlyElementsOf(baselineThreads);
              });
    }
  }

  @Test
  void selectedTimestampVendorsAcceptUtcZoneIdAndTimeZoneProperties() {
    try (SqlDatabase database = SqlDatabase.start()) {
      if (!requiresUtcTimestampBinding(database.store())) {
        try (ConfigurableApplicationContext ignored = application(database)) {
          assertThat(heartbeatCount(database)).isPositive();
          assertJobExecution(ignored, "jdbc-time-zone-" + UUID.randomUUID());
        }
        return;
      }

      for (Object utcTimeZone : List.of(ZoneId.of("UTC"), TimeZone.getTimeZone("GMT"))) {
        try (ConfigurableApplicationContext ignored =
            applicationWithJdbcTimeZone(database, utcTimeZone)) {
          assertThat(heartbeatCount(database)).isPositive();
          assertJobExecution(ignored, "jdbc-time-zone-" + UUID.randomUUID());
        }
      }
    }
  }

  private static ConfigurableApplicationContext application(
      SqlDatabase database, String... commandLineProperties) {
    return new SpringApplicationBuilder(ConsumerApplication.class)
        .properties(database.properties())
        .run(commandLineProperties);
  }

  private static ConfigurableApplicationContext applicationWithJdbcTimeZone(
      SqlDatabase database, Object jdbcTimeZone) {
    return new SpringApplicationBuilder(ConsumerApplication.class)
        .initializers(jdbcTimeZoneInitializer(jdbcTimeZone))
        .properties(database.properties())
        .run();
  }

  private static ApplicationContextInitializer<ConfigurableApplicationContext>
      jdbcTimeZoneInitializer(Object jdbcTimeZone) {
    return context ->
        context
            .getBeanFactory()
            .addBeanPostProcessor(
                new BeanPostProcessor() {
                  @Override
                  public Object postProcessBeforeInitialization(Object bean, String beanName) {
                    if (bean instanceof LocalContainerEntityManagerFactoryBean factory) {
                      factory.getJpaPropertyMap().put("hibernate.jdbc.time_zone", jdbcTimeZone);
                    }
                    return bean;
                  }
                });
  }

  private static String prerequisiteDiagnostic(String store) {
    return switch (store) {
      case "mysql" -> "MySQL session isolation is";
      case "oracle", "sqlserver" ->
          "Ratchet " + store.toUpperCase() + " requires UTC JDBC timestamp binding";
      case "postgresql" -> null;
      default -> throw new IllegalArgumentException("Unsupported SQL consumer store: " + store);
    };
  }

  private static String invalidPrerequisite(String store) {
    return switch (store) {
      case "mysql" -> "--spring.datasource.hikari.transaction-isolation=TRANSACTION_SERIALIZABLE";
      case "oracle", "sqlserver" ->
          "--spring.jpa.properties.hibernate.jdbc.time_zone=America/Los_Angeles";
      default -> throw new IllegalArgumentException("No invalid prerequisite for store: " + store);
    };
  }

  private static boolean requiresUtcTimestampBinding(String store) {
    return store.equals("oracle") || store.equals("sqlserver");
  }

  private static void assertJobExecution(ConfigurableApplicationContext context, String id) {
    context.getBean(ConsumerService.class).submit(id);
    JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForObject(
                            "select state from consumer_record where id = ?", String.class, id))
                    .isEqualTo("executed"));
  }

  private static void assertNoHeartbeat(SqlDatabase database) {
    assertThat(heartbeatCount(database)).isZero();
  }

  private static long heartbeatCount(SqlDatabase database) {
    try (Connection connection = dataSource(database).getConnection()) {
      if (!hasSchedulerNodeTable(connection)) return 0;
      try (Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("select count(*) from scheduler_node")) {
        assertThat(rows.next()).isTrue();
        return rows.getLong(1);
      }
    } catch (SQLException failure) {
      throw new AssertionError("Could not inspect Ratchet node heartbeats", failure);
    }
  }

  private static boolean hasSchedulerNodeTable(Connection connection) throws SQLException {
    try (ResultSet tables =
        connection
            .getMetaData()
            .getTables(
                connection.getCatalog(), connection.getSchema(), "%", new String[] {"TABLE"})) {
      while (tables.next()) {
        if ("scheduler_node".equalsIgnoreCase(tables.getString("TABLE_NAME"))) return true;
      }
      return false;
    }
  }

  private static DriverManagerDataSource dataSource(SqlDatabase database) {
    Map<String, Object> properties = database.properties();
    return new DriverManagerDataSource(
        (String) properties.get("spring.datasource.url"),
        (String) properties.get("spring.datasource.username"),
        (String) properties.get("spring.datasource.password"));
  }

  private static List<String> ratchetThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .map(Thread::getName)
        .filter(name -> name.startsWith("ratchet-standalone-"))
        .sorted()
        .toList();
  }
}
