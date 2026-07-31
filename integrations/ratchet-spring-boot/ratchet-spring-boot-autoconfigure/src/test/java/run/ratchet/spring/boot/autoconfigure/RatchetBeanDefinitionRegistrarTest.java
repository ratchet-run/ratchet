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
package run.ratchet.spring.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import run.ratchet.ri.runtime.RatchetComponentDescriptor;
import run.ratchet.spi.StartupCoordinator;

class RatchetBeanDefinitionRegistrarTest {

  @Test
  void selectedPortableConstructorWinsAndResolvesPlainListAndLazySupplierArguments() {
    AtomicInteger dependencyConstructions = new AtomicInteger();
    PlainDependency plainDependency = new PlainDependency();

    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.registerBean(
          "laterCandidate",
          Candidate.class,
          () -> new Candidate("later", Ordered.LOWEST_PRECEDENCE));
      context.registerBean(
          "firstCandidate",
          Candidate.class,
          () -> new Candidate("first", Ordered.HIGHEST_PRECEDENCE));
      context.registerBean("plainDependency", PlainDependency.class, () -> plainDependency);
      context.registerBean(
          "lazyDependency",
          Dependency.class,
          () -> {
            dependencyConstructions.incrementAndGet();
            return new Dependency("managed");
          },
          definition -> definition.setLazyInit(true));
      registerPortableComponent(context);

      context.refresh();
      PortableConstructorComponent component = context.getBean(PortableConstructorComponent.class);

      assertEquals("portable", component.selectedConstructor);
      assertEquals(
          List.of("first", "later"), component.candidates.stream().map(Candidate::name).toList());
      assertSame(plainDependency, component.plainDependency);
      assertEquals(0, dependencyConstructions.get());
      assertEquals("managed", component.dependencySupplier.get().name());
      assertEquals(1, dependencyConstructions.get());
    }
  }

  @Test
  void explicitGenericSupplierBeanWinsOverSynthesizedProvider() {
    Dependency suppliedDependency = new Dependency("explicit");
    Supplier<Dependency> explicitSupplier = () -> suppliedDependency;

    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.registerBean(PlainDependency.class);
      RootBeanDefinition supplierDefinition = new RootBeanDefinition(Supplier.class);
      supplierDefinition.setTargetType(
          ResolvableType.forClassWithGenerics(Supplier.class, Dependency.class));
      supplierDefinition.setInstanceSupplier(() -> explicitSupplier);
      context.registerBeanDefinition("explicitDependencySupplier", supplierDefinition);
      registerPortableComponent(context);

      context.refresh();
      PortableConstructorComponent component = context.getBean(PortableConstructorComponent.class);

      assertTrue(component.candidates.isEmpty());
      assertSame(explicitSupplier, component.dependencySupplier);
      assertSame(suppliedDependency, component.dependencySupplier.get());
    }
  }

  @Test
  void suppliedInstanceStillReceivesLifecycleAndTransactionalPostProcessing() {
    LifecycleTransactionalComponent.reset();

    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.register(TransactionConfiguration.class);
      RatchetBeanDefinitionRegistrar.registerComponent(
          new RatchetComponentDescriptor(
              LifecycleTransactionalComponent.class, List.of(), true, true),
          context);

      context.refresh();
      assertFalse(LifecycleTransactionalComponent.initialized.get());

      LifecycleTransactionalComponent component =
          context.getBean(LifecycleTransactionalComponent.class);
      RecordingTransactionManager transactionManager =
          context.getBean(RecordingTransactionManager.class);

      assertTrue(LifecycleTransactionalComponent.initialized.get());
      assertTrue(AopUtils.isAopProxy(component));
      component.execute();
      assertEquals(List.of("BEGIN(REQUIRED)", "COMMIT"), transactionManager.events);
    }

    assertTrue(LifecycleTransactionalComponent.destroyed.get());
  }

  @Test
  void replaceableSpiDefaultIsFallbackWhenUserBeanExists() {
    UserStartupCoordinator userCoordinator = new UserStartupCoordinator();
    FallbackStartupCoordinator.constructions.set(0);

    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.registerBean(
          "userStartupCoordinator", StartupCoordinator.class, () -> userCoordinator);
      RatchetBeanDefinitionRegistrar.registerComponent(
          new RatchetComponentDescriptor(FallbackStartupCoordinator.class, List.of(), true, false),
          context);

      context.refresh();

      assertSame(userCoordinator, context.getBean(StartupCoordinator.class));
      assertEquals(0, FallbackStartupCoordinator.constructions.get());
    }
  }

  private static void registerPortableComponent(AnnotationConfigApplicationContext context) {
    RatchetBeanDefinitionRegistrar.registerComponent(
        new RatchetComponentDescriptor(
            PortableConstructorComponent.class,
            List.of(List.class, Supplier.class, PlainDependency.class),
            true,
            false),
        context);
  }

  static final class Candidate implements Ordered {

    private final String name;
    private final int order;

    Candidate(String name, int order) {
      this.name = name;
      this.order = order;
    }

    String name() {
      return name;
    }

    @Override
    public int getOrder() {
      return order;
    }
  }

  static final class Dependency {

    private final String name;

    Dependency(String name) {
      this.name = name;
    }

    String name() {
      return name;
    }
  }

  static final class PlainDependency {}

  static class UserStartupCoordinator implements StartupCoordinator {

    @Override
    public boolean tryAcquire(String actionName, Duration leaseTtl) {
      return true;
    }

    @Override
    public void release(String actionName) {}
  }

  static final class FallbackStartupCoordinator extends UserStartupCoordinator {

    private static final AtomicInteger constructions = new AtomicInteger();

    FallbackStartupCoordinator() {
      constructions.incrementAndGet();
    }
  }

  static final class PortableConstructorComponent {

    private final String selectedConstructor;
    private final List<Candidate> candidates;
    private final Supplier<Dependency> dependencySupplier;
    private final PlainDependency plainDependency;

    @Inject
    PortableConstructorComponent(Instance<Candidate> candidates) {
      this.selectedConstructor = "inject";
      this.candidates = List.of();
      this.dependencySupplier = () -> null;
      this.plainDependency = null;
    }

    PortableConstructorComponent(
        List<Candidate> candidates,
        Supplier<Dependency> dependencySupplier,
        PlainDependency plainDependency) {
      this.selectedConstructor = "portable";
      this.candidates = candidates;
      this.dependencySupplier = dependencySupplier;
      this.plainDependency = plainDependency;
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement(proxyTargetClass = true)
  static class TransactionConfiguration {

    @Bean
    RecordingTransactionManager transactionManager() {
      return new RecordingTransactionManager();
    }
  }

  static class LifecycleTransactionalComponent {

    private static final AtomicBoolean initialized = new AtomicBoolean();
    private static final AtomicBoolean destroyed = new AtomicBoolean();

    static void reset() {
      initialized.set(false);
      destroyed.set(false);
    }

    @PostConstruct
    void initialize() {
      initialized.set(true);
    }

    @PreDestroy
    void destroy() {
      destroyed.set(true);
    }

    @Transactional
    public void execute() {}
  }

  static final class RecordingTransactionManager extends AbstractPlatformTransactionManager {

    private final List<String> events = new ArrayList<>();

    @Override
    protected Object doGetTransaction() throws TransactionException {
      return new Object();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition)
        throws TransactionException {
      events.add("BEGIN(REQUIRED)");
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
      events.add("COMMIT");
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
      events.add("ROLLBACK");
    }
  }
}
