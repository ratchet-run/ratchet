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
package example.ratchet.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.consumer.mongodb.MongoDatabase;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

class CircuitBreakerBeanTest {
  @Test
  void protectsWithoutJpaTransactionManagerOrBootAop() {
    try (MongoDBContainer database = MongoDatabase.create()) {
      database.start();
      try (var context =
          new SpringApplicationBuilder(MongoConsumerApplication.class)
              .sources(ProtectedBeans.class)
              .properties(
                  MongoConsumerProperties.connection(database.getReplicaSetUrl("ratchet_consumer")))
              .properties("spring.aop.auto=false")
              .run()) {
        assertThat(context.getBeansOfType(PlatformTransactionManager.class)).isEmpty();
        var service = context.getBean(ProtectedService.class);
        assertThat(AopUtils.isAopProxy(service)).isTrue();
        for (int i = 0; i < 3; i++) assertThrows(IllegalStateException.class, service::call);
        assertThrows(CircuitBreakerOpenException.class, service::call);
        assertThat(service.calls()).isEqualTo(3);
      }
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ProtectedBeans {
    @Bean
    ProtectedService protectedService() {
      return new ProtectedService();
    }

    @Bean
    @Primary
    CircuitBreakerConfigProvider policy() {
      return new CircuitBreakerConfigProvider() {
        @Override
        public boolean isEnabled() {
          return true;
        }

        @Override
        public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
          return new CircuitBreakerConfig(50, 20, 30000, 2, 3);
        }
      };
    }
  }

  public static class ProtectedService {
    private int calls;

    @CircuitBreakerProtected(service = "mongo-bean")
    public void call() {
      calls++;
      throw new IllegalStateException();
    }

    public int calls() {
      return calls;
    }
  }
}
