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

import example.ratchet.ConsumerApplication;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.transaction.annotation.Transactional;
import run.ratchet.api.*;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;

class DeferredBeansRuntimeIT {
  static final AtomicInteger created = new AtomicInteger();
  static final AtomicInteger destroyed = new AtomicInteger();
  static final AtomicInteger factoryShutdowns = new AtomicInteger();
  static final AtomicInteger invoked = new AtomicInteger();

  @ParameterizedTest
  @ValueSource(strings = {"lazy", "prototype", "factory"})
  void unexposedDeferredTargetFailsOnceWithoutPrematureConstruction(String scope) {
    created.set(0);
    destroyed.set(0);
    factoryShutdowns.set(0);
    invoked.set(0);
    try (var database = SqlDatabase.start()) {
      var properties = RuntimeSupport.properties(database);
      properties.put("fixture.scope", scope);
      properties.put("spring.aop.proxy-target-class", false);
      try (var context =
          new SpringApplicationBuilder(ConsumerApplication.class, Configuration.class)
              .properties(properties)
              .run()) {
        assertThat(created).hasValue(0);
        var handle =
            context
                .getBean(InvocationSubmissionService.class)
                .enqueueInvocation(
                    new JobInvocation(
                        DeferredTask.class.getName(), "unavailable", "()V", false, List.of()))
                .withMaxRetries(5)
                .submit();
        RuntimeSupport.status(context, handle, JobStatus.FAILED);
        var queries = context.getBean(JobQueryService.class);
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> assertThat(queries.getExecutionHistory(handle.id())).hasSize(1));
        assertThat(queries.getExecutionHistory(handle.id()).get(0).errorMessage())
            .contains("unavailable");
        assertThat(created).hasValue(1);
        assertThat(invoked).hasValue(0);
        if (!scope.equals("lazy")) assertThat(destroyed).hasValue(1);
        if (scope.equals("factory")) assertThat(factoryShutdowns).hasValue(0);
        // Leave several polling cycles to detect an accidental retry instead of a terminal state.
        await()
            .during(Duration.ofSeconds(2))
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(
                () -> {
                  assertThat(queries.getExecutionHistory(handle.id())).hasSize(1);
                  assertThat(created).hasValue(1);
                });
      }
      assertThat(destroyed).hasValue(1);
      assertThat(factoryShutdowns).hasValue(scope.equals("factory") ? 1 : 0);
    }
  }

  public static class DeferredTask implements Runnable {
    public DeferredTask() {
      created.incrementAndGet();
    }

    @Override
    @Transactional
    public void run() {
      invoked.incrementAndGet();
    }

    @Recurring(cron = "0 0 0 1 1 ?")
    public void unavailable() {
      invoked.incrementAndGet();
    }

    @PreDestroy
    public void destroy() {
      destroyed.incrementAndGet();
    }
  }

  public static class DeferredFactory implements SmartFactoryBean<DeferredTask> {
    @Override
    public DeferredTask getObject() {
      return new DeferredTask();
    }

    @Override
    public Class<?> getObjectType() {
      return DeferredTask.class;
    }

    @Override
    public boolean isSingleton() {
      return false;
    }

    @Override
    public boolean isPrototype() {
      return true;
    }

    public void shutdownFactory() {
      factoryShutdowns.incrementAndGet();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Configuration {
    @Bean
    static BeanDefinitionRegistryPostProcessor deferredDefinition(Environment environment) {
      return registry -> {
        String scope = environment.getRequiredProperty("fixture.scope");
        var definition =
            new RootBeanDefinition(
                scope.equals("factory") ? DeferredFactory.class : DeferredTask.class);
        definition.setLazyInit(true);
        if (scope.equals("prototype")) definition.setScope("prototype");
        if (scope.equals("factory")) definition.setDestroyMethodName("shutdownFactory");
        registry.registerBeanDefinition("deferredTask", definition);
      };
    }
  }
}
