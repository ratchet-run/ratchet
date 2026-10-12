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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

/** Application beans shared by JVM, AOT and native circuit-breaker verification. */
@Configuration(proxyBeanMethods = false)
@Profile("verification")
public class CircuitBreakerBeanConfiguration {
  @Bean
  @Primary
  CircuitBreakerConfigProvider consumerCircuitBreakerConfig() {
    return new CircuitBreakerConfigProvider() {
      @Override
      public boolean isEnabled() {
        return true;
      }

      @Override
      public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
        return new CircuitBreakerConfig(50, 20, 1000, 2, 3);
      }
    };
  }

  @Bean
  ProtectedService protectedService() {
    return new ProtectedService();
  }

  @Bean
  ClassProtectedService classProtectedService() {
    return new ClassProtectedService();
  }

  @Bean
  InheritedProtectedService inheritedProtectedService() {
    return new InheritedProtectedService();
  }

  public static class ProtectedService {
    private int calls;
    private int transactionalCalls;

    @CircuitBreakerProtected(service = "consumer-service")
    public String call(boolean fail) {
      calls++;
      if (fail) throw new IllegalStateException("failure");
      return "ok";
    }

    public int calls() {
      return calls;
    }

    public int transactionalCalls() {
      return transactionalCalls;
    }

    @CircuitBreakerProtected(service = "ignored", ignoreExceptions = IllegalArgumentException.class)
    public void ignored() {
      throw new IllegalArgumentException("ignored");
    }

    @CircuitBreakerProtected
    public void defaultCall() {
      throw new IllegalStateException("default");
    }

    @Transactional
    @CircuitBreakerProtected(service = "transactional")
    public void transactionalCall() {
      if (!TransactionSynchronizationManager.isActualTransactionActive()) {
        throw new AssertionError("transaction advice did not run");
      }
      transactionalCalls++;
      throw new IllegalStateException("transactional");
    }
  }

  @CircuitBreakerProtected(service = "class-service")
  public static class ClassProtectedService {
    public void call() {
      throw new IllegalStateException("class");
    }
  }

  public static class InheritedProtectedService extends ClassProtectedService {
    @Override
    public void call() {
      throw new IllegalStateException("inherited");
    }
  }
}
