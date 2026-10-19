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
package run.ratchet.micronaut;

import static org.junit.jupiter.api.Assertions.*;

import io.micronaut.aop.Intercepted;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;

class MicronautBeanResolverTest {
  interface Job {}

  static class First implements Job {}

  static class Other implements Job {}

  static class PrototypeJob {
    static final AtomicInteger created = new AtomicInteger();
    static final AtomicInteger destroyed = new AtomicInteger();

    PrototypeJob() {
      created.incrementAndGet();
    }

    @PreDestroy
    public void close() {
      destroyed.incrementAndGet();
    }
  }

  public static class AdvisedJob {
    public void run() {}
  }

  @Factory
  @Requires(property = "test.resolver", value = "true")
  static class Jobs {
    @Singleton
    @Primary
    @Named("first")
    Job first() {
      return new First();
    }

    @Singleton
    @Named("other")
    Job other() {
      return new Other();
    }

    @Singleton
    @RatchetTransactionBoundary
    AdvisedJob advisedJob() {
      return new AdvisedJob();
    }

    @Prototype
    @Bean(preDestroy = "close")
    PrototypeJob prototypeJob() {
      return new PrototypeJob();
    }
  }

  @Test
  void primaryAndQualifiersArePreserved() {
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false, "test.resolver", true))) {
      context.start();
      var resolver = new MicronautBeanResolver(context);
      resolver.validateResolvable(Job.class);
      assertInstanceOf(First.class, resolver.resolve(Job.class));
      try (var handle = resolver.acquire(Job.class)) {
        assertSame(resolver.resolve(Job.class), handle.instance());
      }
      assertInstanceOf(First.class, resolver.resolve(Job.class));
    }
  }

  @Test
  void invocationHandlesRetainMicronautAdvice() {
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false, "test.resolver", true))) {
      context.start();
      var resolver = new MicronautBeanResolver(context);
      try (var handle = resolver.acquire(AdvisedJob.class)) {
        assertInstanceOf(Intercepted.class, handle.instance());
        assertSame(resolver.resolve(AdvisedJob.class), handle.instance());
        ((AdvisedJob) handle.instance()).run();
      }
    }
  }

  @Test
  void validatesWithoutInstantiationAndDestroysEachPrototypeOnce() {
    PrototypeJob.created.set(0);
    PrototypeJob.destroyed.set(0);
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false, "test.resolver", true))) {
      context.start();
      var resolver = new MicronautBeanResolver(context);
      resolver.validateResolvable(PrototypeJob.class);
      assertEquals(0, PrototypeJob.created.get());
      assertThrows(IllegalStateException.class, () -> resolver.resolve(PrototypeJob.class));
      var first = resolver.acquire(PrototypeJob.class);
      var second = resolver.acquire(PrototypeJob.class);
      assertNotSame(first.instance(), second.instance());
      first.close();
      first.close();
      second.close();
      assertEquals(2, PrototypeJob.destroyed.get());
    }
  }

  @Test
  void missingAndAmbiguousTypesFailWithoutConstructingBeans() {
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false))) {
      context.registerSingleton(Job.class, new First(), Qualifiers.byName("first"));
      context.registerSingleton(Job.class, new Other(), Qualifiers.byName("other"));
      context.start();
      var resolver = new MicronautBeanResolver(context);
      assertThrows(RuntimeException.class, () -> resolver.validateResolvable(Job.class));
      assertThrows(RuntimeException.class, () -> resolver.validateResolvable(PrototypeJob.class));
    }
  }
}
