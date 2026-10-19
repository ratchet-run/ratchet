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
import static org.awaitility.Awaitility.await;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.consumer.sql.SqlAccess;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ExecutorProvider;

/** Compiled by each lane's processor against the same 4.10-built integration jars. */
class MicronautOverridesTest {
  @Test
  void replacementsMissingBeanOverridesNamedInjectionAndPublicSchedulerWork() {
    try (var database = SqlDatabase.start();
        var context = SqlApplication.start(database, "consumer.overrides=true")) {
      var app = context.getBean(ConsumerInjection.class);
      var executors = context.getBean(ApplicationExecutors.class);
      var policy = context.getBean(ConsumerPolicy.class);
      assertThat(context.getBeansOfType(ExecutorProvider.class)).containsExactly(executors);
      assertThat(context.getBeansOfType(ClassPolicy.class)).containsExactly(policy);
      assertThat(app.executors).isSameAs(executors);
      assertThat(app.scheduler).isSameAs(context.getBean(JobSchedulerService.class));
      context.getBean(ConsumerService.class).submit("overrides");
      var jdbc = new SqlAccess(context);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(
                        jdbc.queryForObject(
                            "select state from consumer_record where id = 'overrides'",
                            String.class))
                    .isEqualTo("executed");
                assertThat(policy.calls.get()).isPositive();
              });
    }
  }

  @Singleton
  @Requires(property = "consumer.overrides", value = "true")
  public static class ConsumerInjection {
    final JobSchedulerService scheduler;
    final ExecutorProvider executors;

    public ConsumerInjection(
        JobSchedulerService scheduler, @Named("consumerExecutors") ExecutorProvider executors) {
      this.scheduler = scheduler;
      this.executors = executors;
    }
  }

  @Factory
  @Requires(property = "consumer.overrides", value = "true")
  static class Overrides {
    @Singleton
    @Named("consumerExecutors")
    @Replaces(ExecutorProvider.class)
    @Bean(preDestroy = "close")
    ApplicationExecutors executors() {
      return new ApplicationExecutors();
    }

    // No @Replaces: the integration's missing-bean condition must back off.
    @Singleton
    ConsumerPolicy classPolicy() {
      return new ConsumerPolicy();
    }
  }

  public static final class ConsumerPolicy implements ClassPolicy {
    final AtomicInteger calls = new AtomicInteger();

    public boolean isAllowed(String className) {
      calls.incrementAndGet();
      return className != null && className.startsWith("example.ratchet.");
    }
  }

  public static final class ApplicationExecutors implements ExecutorProvider, AutoCloseable {
    final ExecutorService workers = Executors.newCachedThreadPool();
    final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2);

    public ExecutorService getJobExecutor() {
      return workers;
    }

    public ScheduledExecutorService getScheduledExecutor() {
      return timers;
    }

    public void close() {
      workers.shutdownNow();
      timers.shutdownNow();
    }
  }
}
