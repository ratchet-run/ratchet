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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.spring.boot.autoconfigure.SpringBeanResolver;

/** Five cross-Boot contracts for Spring bean selection and managed lifetime. */
class SpringBeanResolverContractTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
          .withPropertyValues("ratchet.allow-empty-class-policy=true");

  @Test
  void singletonHandleCloseLeavesContainerOwnedBeanAlive() {
    SingletonProbe.destroyCount.set(0);

    contextRunner
        .withUserConfiguration(SingletonConfiguration.class)
        .run(
            context -> {
              BeanResolver resolver = context.getBean(BeanResolver.class);
              assertInstanceOf(SpringBeanResolver.class, resolver);
              SingletonProbe singleton = context.getBean(SingletonProbe.class);

              try (BeanResolver.ManagedBeanHandle<SingletonProbe> handle =
                  resolver.resolveManaged(SingletonProbe.class)) {
                assertSame(singleton, handle.get());
              }

              assertEquals(0, SingletonProbe.destroyCount.get());
            });
  }

  @Test
  void prototypeHandleCloseDestroysExactlyThatResolution() {
    PrototypeProbe.destroyCount.set(0);

    contextRunner
        .withUserConfiguration(PrototypeConfiguration.class)
        .run(
            context -> {
              BeanResolver resolver = context.getBean(BeanResolver.class);
              BeanResolver.ManagedBeanHandle<PrototypeProbe> handle =
                  resolver.resolveManaged(PrototypeProbe.class);

              assertEquals(0, PrototypeProbe.destroyCount.get());
              handle.close();
              handle.close();

              assertEquals(1, PrototypeProbe.destroyCount.get());
            });
  }

  @Test
  void missingBeanReportsTheRequestedType() {
    contextRunner.run(
        context -> {
          BeanResolver resolver = context.getBean(BeanResolver.class);

          IllegalStateException failure =
              assertThrows(IllegalStateException.class, () -> resolver.resolve(MissingProbe.class));

          assertEquals(
              "No Spring bean found for type: " + MissingProbe.class.getName(),
              failure.getMessage());
        });
  }

  @Test
  void multipleBeansWithoutPrimaryAreRejected() {
    contextRunner
        .withUserConfiguration(AmbiguousConfiguration.class)
        .run(
            context -> {
              BeanResolver resolver = context.getBean(BeanResolver.class);

              IllegalStateException failure =
                  assertThrows(
                      IllegalStateException.class, () -> resolver.resolve(AmbiguousProbe.class));

              assertEquals(
                  "Multiple Spring beans found for type: "
                      + AmbiguousProbe.class.getName()
                      + ". Use @Primary to disambiguate.",
                  failure.getMessage());
            });
  }

  @Test
  void onePrimaryBeanWinsAmongMultipleCandidates() {
    contextRunner
        .withUserConfiguration(PrimaryConfiguration.class)
        .run(
            context -> {
              BeanResolver resolver = context.getBean(BeanResolver.class);

              assertSame(context.getBean("primaryProbe"), resolver.resolve(SelectionProbe.class));
            });
  }

  @Configuration(proxyBeanMethods = false)
  static class SingletonConfiguration {

    @Bean
    SingletonProbe singletonProbe() {
      return new SingletonProbe();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class PrototypeConfiguration {

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    PrototypeProbe prototypeProbe() {
      return new PrototypeProbe();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class AmbiguousConfiguration {

    @Bean
    AmbiguousProbe firstAmbiguousProbe() {
      return new AmbiguousProbe();
    }

    @Bean
    AmbiguousProbe secondAmbiguousProbe() {
      return new AmbiguousProbe();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class PrimaryConfiguration {

    @Bean
    @Primary
    SelectionProbe primaryProbe() {
      return new SelectionProbe("primary");
    }

    @Bean
    SelectionProbe secondaryProbe() {
      return new SelectionProbe("secondary");
    }
  }

  static final class SingletonProbe implements DisposableBean {

    private static final AtomicInteger destroyCount = new AtomicInteger();

    @Override
    public void destroy() {
      destroyCount.incrementAndGet();
    }
  }

  static final class PrototypeProbe implements DisposableBean {

    private static final AtomicInteger destroyCount = new AtomicInteger();

    @Override
    public void destroy() {
      destroyCount.incrementAndGet();
    }
  }

  static final class MissingProbe {}

  static final class AmbiguousProbe {}

  record SelectionProbe(String name) {}
}
