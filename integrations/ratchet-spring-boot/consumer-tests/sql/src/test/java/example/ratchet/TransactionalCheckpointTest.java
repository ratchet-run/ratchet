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
package example.ratchet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;

class TransactionalCheckpointTest {
  @Test
  void retryResumesAfterLastCommittedUnit() {
    verify("retry");
  }

  @Test
  void failedUnitRollsBackRowAndCheckpoint() {
    verify("failure");
  }

  @Test
  void enclosingTransactionIsRejected() {
    verify("enclosing");
  }

  @Test
  void otherManagerWriteInsideUnitCommitsOnItsOwn() {
    verify("other");
  }

  @Test
  void unitHoldsOneConnectionAtATime() {
    verify("connections");
  }

  private void verify(String scenario) {
    try (var database = SqlDatabase.start();
        var context =
            new SpringApplicationBuilder(ConsumerApplication.class, UnitConfiguration.class)
                .properties(database.properties())
                .run()) {
      var jobs = context.getBean(UnitJobs.class);
      var scheduler = context.getBean(JobSchedulerService.class);
      var store = context.getBean(JobStore.class);
      var properties = database.properties();
      var probe =
          new JdbcTemplate(
              new DriverManagerDataSource(
                  (String) properties.get("spring.datasource.url"),
                  (String) properties.get("spring.datasource.username"),
                  (String) properties.get("spring.datasource.password")));
      String prefix = UUID.randomUUID().toString();
      UUID id =
          scheduler
              .enqueue(() -> jobs.execute(prefix, scenario))
              .withMaxRetries(1)
              .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
              .submit()
              .id();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(store.findById(id).orElseThrow().getStatus())
                      .isEqualTo(JobStatus.SUCCEEDED));
      var state =
          store.capability(JobStateStore.class).orElseThrow().getState(id, "ratchet.checkpoint");
      if (scenario.equals("retry") || scenario.equals("connections")) {
        for (int page = 0; page < 3; page++)
          assertThat(count(probe, prefix + "-" + page)).isEqualTo(1);
        assertThat(
                context
                    .getBean(PayloadSerializer.class)
                    .deserialize(state.orElseThrow().json(), Map.class)
                    .get("value"))
            .isEqualTo("done");
        assertThat(jobs.received)
            .containsExactly(Optional.empty(), Optional.of("1"), Optional.of("2"));
        if (scenario.equals("retry")) assertThat(jobs.retryFirst).isEqualTo("2");
        else assertThat(jobs.maximum).isEqualTo(1);
      } else if (scenario.equals("failure")) {
        assertThat(count(probe, prefix + "-0")).isEqualTo(1);
        assertThat(count(probe, prefix + "-1")).isZero();
        assertThat(jobs.afterFailure).isEqualTo(Optional.of("1"));
        assertThat(jobs.caught).isSameAs(jobs.failure);
        assertThat(
                context
                    .getBean(PayloadSerializer.class)
                    .deserialize(state.orElseThrow().json(), Map.class)
                    .get("value"))
            .isEqualTo("1");
      } else {
        assertThat(state).isEmpty();
        assertThat(count(probe, prefix + "-0")).isZero();
        if (scenario.equals("other")) {
          assertThat(count(probe, prefix + "-other")).isEqualTo(1);
          assertThat(jobs.caught).isSameAs(jobs.failure);
        } else assertThat(jobs.unitRan).isFalse();
      }
    }
  }

  private static int count(JdbcTemplate jdbc, String id) {
    return jdbc.queryForObject(
        "select count(*) from consumer_record where id = ?", Integer.class, id);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class UnitConfiguration {
    @Bean
    static BeanPostProcessor countingDataSource() {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String name) {
          return bean instanceof DataSource source ? new CountingDataSource(source) : bean;
        }
      };
    }

    @Bean
    UnitJobs unitJobs(
        JdbcTemplate jdbc, PlatformTransactionManager manager, Environment environment) {
      // Keep the second manager out of the bean registry so Ratchet's selection is unchanged.
      var other =
          new DriverManagerDataSource(
              environment.getRequiredProperty("spring.datasource.url"),
              environment.getRequiredProperty("spring.datasource.username"),
              environment.getRequiredProperty("spring.datasource.password"));
      return new UnitJobs(jdbc, manager, other);
    }
  }

  public static class UnitJobs {
    private final JdbcTemplate jdbc;
    private final JdbcTemplate otherJdbc;
    private final TransactionTemplate transaction;
    private final TransactionTemplate otherTransaction;
    private final AtomicInteger attempts = new AtomicInteger();
    final List<Optional<String>> received = new CopyOnWriteArrayList<>();
    final IOException failure = new IOException("unit failure");
    volatile Exception caught;
    volatile Optional<String> afterFailure;
    volatile String retryFirst;
    volatile boolean unitRan;
    volatile int maximum;

    UnitJobs(JdbcTemplate jdbc, PlatformTransactionManager manager, DataSource other) {
      this.jdbc = jdbc;
      transaction = new TransactionTemplate(manager);
      otherJdbc = new JdbcTemplate(other);
      otherTransaction = new TransactionTemplate(new DataSourceTransactionManager(other));
    }

    public void execute(String prefix, String scenario) throws Exception {
      var ctx = JobContext.current();
      int attempt = attempts.incrementAndGet();
      if (scenario.equals("enclosing")) {
        for (var template : List.of(transaction, otherTransaction)) {
          template.executeWithoutResult(
              status ->
                  assertThatThrownBy(
                          () ->
                              ctx.runInTransaction(
                                  cp -> {
                                    unitRan = true;
                                    insert(jdbc, prefix + "-0");
                                    return "1";
                                  }))
                      .isInstanceOf(IllegalStateException.class));
        }
        return;
      }
      if (scenario.equals("other")) {
        try {
          ctx.runInTransaction(
              cp -> {
                // Only Ratchet's datasource is inside the unit's transaction.
                otherTransaction.executeWithoutResult(
                    status -> insert(otherJdbc, prefix + "-other"));
                insert(jdbc, prefix + "-0");
                throw failure;
              });
        } catch (IOException exception) {
          caught = exception;
        }
        return;
      }
      if (scenario.equals("failure")) {
        ctx.runInTransaction(
            cp -> {
              insert(jdbc, prefix + "-0");
              return "1";
            });
        try {
          ctx.runInTransaction(
              cp -> {
                insert(jdbc, prefix + "-1");
                throw failure;
              });
        } catch (IOException exception) {
          caught = exception;
        }
        afterFailure = ctx.lastCheckpoint();
        return;
      }
      boolean first = true;
      while (true) {
        boolean firstUnit = first;
        String next =
            ctx.runInTransaction(
                cp -> {
                  received.add(Optional.ofNullable(cp));
                  if (attempt == 2 && firstUnit) retryFirst = cp;
                  int page = cp == null ? 0 : Integer.parseInt(cp);
                  // A unit must not need a second connection, or a full pool would deadlock.
                  ConnectionCounts counts = CountingDataSource.COUNTS.get();
                  counts.maximum = counts.open;
                  counts.measuring = true;
                  try {
                    insert(jdbc, prefix + "-" + page);
                    ctx.lastCheckpoint();
                    ctx.progress(page + 1, 3);
                    maximum = Math.max(maximum, counts.maximum);
                    return page == 2 ? "done" : Integer.toString(page + 1);
                  } finally {
                    counts.measuring = false;
                  }
                });
        first = false;
        if (scenario.equals("retry") && attempt == 1 && next.equals("2"))
          throw new IOException("Retry after the committed unit");
        if (next.equals("done")) return;
      }
    }

    private static void insert(JdbcTemplate jdbc, String id) {
      jdbc.update("insert into consumer_record (id, state) values (?, ?)", id, "unit");
    }
  }

  static final class ConnectionCounts {
    int open;
    int maximum;
    boolean measuring;
  }

  static final class CountingDataSource extends DelegatingDataSource {
    static final ThreadLocal<ConnectionCounts> COUNTS =
        ThreadLocal.withInitial(ConnectionCounts::new);

    CountingDataSource(DataSource source) {
      super(source);
    }

    @Override
    public Connection getConnection() throws SQLException {
      return track(super.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return track(super.getConnection(username, password));
    }

    private Connection track(Connection connection) {
      ConnectionCounts counts = COUNTS.get();
      counts.open++;
      if (counts.measuring) counts.maximum = Math.max(counts.maximum, counts.open);
      AtomicBoolean closed = new AtomicBoolean();
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, arguments) -> {
                try {
                  return method.invoke(connection, arguments);
                } catch (InvocationTargetException failure) {
                  throw failure.getCause();
                } finally {
                  if (method.getName().equals("close") && closed.compareAndSet(false, true))
                    counts.open--;
                }
              });
    }
  }
}
