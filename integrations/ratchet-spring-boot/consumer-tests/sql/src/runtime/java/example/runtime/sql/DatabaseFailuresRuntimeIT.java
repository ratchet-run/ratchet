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
package example.runtime.sql;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import example.ratchet.ConsumerApplication;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import run.ratchet.api.*;
import run.ratchet.consumer.TcpFaultProxy;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.ri.core.DrainController;

public class DatabaseFailuresRuntimeIT {
  static final java.util.concurrent.atomic.AtomicInteger executions =
      new java.util.concurrent.atomic.AtomicInteger();
  static CountDownLatch entered;
  static CountDownLatch release;

  @Test
  void databaseDisconnectDuringPollingHeartbeatAndCompletionRecoversInSameContext()
      throws Exception {
    try (var database = SqlDatabase.start()) {
      String original = (String) database.properties().get("spring.datasource.url");
      URI endpoint = URI.create(original.substring(5));
      try (var wire = new TcpFaultProxy(endpoint.getHost(), endpoint.getPort())) {
        var properties = RuntimeSupport.properties(database);
        properties.put(
            "spring.datasource.url",
            original.replace(
                    endpoint.getHost() + ":" + endpoint.getPort(), "127.0.0.1:" + wire.port())
                + (original.contains("?") ? "&" : "?")
                + "socketTimeout=2&connectTimeout=2");
        // Keep the three-second outage inside the existing bounded finalization retry window,
        // including the pool's exponential delay before creating replacement connections.
        properties.put("spring.datasource.hikari.connection-timeout", 2000);
        properties.put("ratchet.node.heartbeat-interval-seconds", 1);
        executions.set(0);
        entered = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (var context =
            new SpringApplicationBuilder(ConsumerApplication.class).properties(properties).run()) {
          var scheduler = context.getBean(JobSchedulerService.class);
          var running =
              scheduler.enqueue(DatabaseFailuresRuntimeIT::blocked).withMaxRetries(2).submit();
          assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
          context.getBean(DrainController.class).setDraining(true);
          var pending = scheduler.enqueue(RuntimeSupport::noop).submit();
          wire.disconnect();
          release.countDown();
          context.getBean(DrainController.class).setDraining(false);
          await().atMost(Duration.ofSeconds(10)).until(() -> wire.rejected.get() > 0);
          await()
              .during(Duration.ofSeconds(3))
              .atMost(Duration.ofSeconds(5))
              .until(() -> context.isActive());
          wire.reconnect();
          await()
              .atMost(Duration.ofSeconds(20))
              .ignoreExceptionsInstanceOf(
                  org.springframework.jdbc.CannotGetJdbcConnectionException.class)
              .untilAsserted(
                  () ->
                      assertThat(
                              new org.springframework.jdbc.core.JdbcTemplate(
                                      context.getBean(javax.sql.DataSource.class))
                                  .queryForObject("select 1", Integer.class))
                          .isEqualTo(1));
          RuntimeSupport.status(context, pending, JobStatus.SUCCEEDED);
          RuntimeSupport.status(context, running, JobStatus.SUCCEEDED);
          assertThat(executions).hasValue(1);
          RuntimeSupport.status(
              context, scheduler.enqueue(RuntimeSupport::noop).submit(), JobStatus.SUCCEEDED);
        } finally {
          release.countDown();
          wire.reconnect();
        }
      }
    }
  }

  @Test
  void poolExhaustionFailsWithinTimeoutAndWorkersRecoverAfterConnectionsReturn() throws Exception {
    try (var database = SqlDatabase.start()) {
      var properties = RuntimeSupport.properties(database);
      properties.put("spring.datasource.hikari.maximum-pool-size", 2);
      properties.put("spring.datasource.hikari.minimum-idle", 2);
      properties.put("spring.datasource.hikari.connection-timeout", 1000);
      try (var context =
          new SpringApplicationBuilder(ConsumerApplication.class).properties(properties).run()) {
        var pool = context.getBean(HikariDataSource.class);
        var scheduler = context.getBean(JobSchedulerService.class);
        context.getBean(DrainController.class).setDraining(true);
        var pending = scheduler.enqueue(RuntimeSupport::noop).submit();
        try (var first = pool.getConnection();
            var second = pool.getConnection()) {
          context.getBean(DrainController.class).setDraining(false);
          var rejected =
              CompletableFuture.supplyAsync(
                  () -> catchThrowable(() -> scheduler.enqueue(RuntimeSupport::noop).submit()));
          await()
              .atMost(Duration.ofSeconds(3))
              .until(() -> pool.getHikariPoolMXBean().getThreadsAwaitingConnection() > 0);
          assertThat(rejected.get(5, TimeUnit.SECONDS))
              .isNotNull()
              .hasStackTraceContaining("Connection is not available");
          assertThat(pool.getHikariPoolMXBean().getTotalConnections()).isEqualTo(2);
        }
        RuntimeSupport.status(context, pending, JobStatus.SUCCEEDED);
        RuntimeSupport.status(
            context, scheduler.enqueue(RuntimeSupport::noop).submit(), JobStatus.SUCCEEDED);
        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> pool.getHikariPoolMXBean().getThreadsAwaitingConnection() == 0);
      }
    }
  }

  public static void blocked() throws Exception {
    executions.incrementAndGet();
    entered.countDown();
    if (!release.await(30, TimeUnit.SECONDS))
      throw new IllegalStateException("fixture not released");
  }
}
