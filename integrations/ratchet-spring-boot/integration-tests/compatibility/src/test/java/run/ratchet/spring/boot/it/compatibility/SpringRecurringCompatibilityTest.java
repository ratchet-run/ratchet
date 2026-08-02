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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.RecurringMisfirePolicy;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringMethodRegistrar;
import run.ratchet.ri.core.internal.RecurringRegistration;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.spring.boot.autoconfigure.SpringBeanResolver;
import run.ratchet.spring.boot.autoconfigure.SpringRecurringMethodDiscovery;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;

@ExtendWith(OutputCaptureExtension.class)
public class SpringRecurringCompatibilityTest {

  @Test
  void cglibProxyUsesCatalogWiringAndStableUserClassInPersistedPayload() throws Exception {
    CglibRecurringBean target = new CglibRecurringBean();
    ProxyFactory proxyFactory = new ProxyFactory(target);
    proxyFactory.setProxyTargetClass(true);
    Object proxy = proxyFactory.getProxy();
    assertTrue(AopUtils.isCglibProxy(proxy));

    try (Fixture fixture = fixture("cglibBean", CglibRecurringBean.class, () -> proxy, false)) {
      assertInstanceOf(SpringBeanResolver.class, fixture.context.getBean(BeanResolver.class));
      assertInstanceOf(
          SpringRecurringMethodDiscovery.class,
          fixture.context.getBean(RecurringMethodDiscovery.class));
      assertSame(fixture.context.getBean(RecurringMethodInvoker.class), fixture.methodInvoker);
      assertSame(fixture.registrar, fixture.registration);

      fixture.registration.register();

      RecordedRecurringInvocation submission = fixture.onlySubmission();
      assertEquals("0 0/5 * * * ?", submission.cron);
      assertEquals(ZoneId.of("UTC"), submission.zone);
      assertEquals("spring-cglib", submission.businessKey);
      assertEquals(RecurringMethodInvoker.class.getName(), submission.invocation.targetClass());
      assertEquals("invoke", submission.invocation.methodName());
      assertEquals(
          "(Ljava/lang/String;Ljava/lang/String;Z)V", submission.invocation.methodDescriptor());
      assertFalse(submission.invocation.staticMethod());
      assertEquals(
          List.of(CglibRecurringBean.class.getName(), "run", false),
          submission.invocation.arguments());
      assertFalse(
          String.valueOf(submission.invocation.arguments().get(0)).contains("$$SpringCGLIB"));

      fixture.invokeOnlyInvocation();
      assertEquals(1, target.invocations.get());
    }
  }

  @Test
  void jdkProxyWithInterfaceMethodIsDiscoveredAndInvocable() throws Exception {
    JdkRecurringBean target = new JdkRecurringBean();
    ProxyFactory proxyFactory = new ProxyFactory(target);
    Object proxy = proxyFactory.getProxy();
    assertTrue(AopUtils.isJdkDynamicProxy(proxy));

    try (Fixture fixture = fixture("jdkBean", JdkRecurringBean.class, () -> proxy, false)) {
      fixture.registration.register();

      assertEquals(
          recurringInvocation(JdkRecurringBean.class, "run", false), fixture.onlyInvocation());
      fixture.invokeOnlyInvocation();
      assertEquals(1, target.invocations.get());
    }
  }

  @Test
  void jdkProxyWithClassOnlyMethodIsSkippedWithGuidance(CapturedOutput output) {
    JdkClassOnlyBean target = new JdkClassOnlyBean();
    ProxyFactory proxyFactory = new ProxyFactory(target);
    Object proxy = proxyFactory.getProxy();
    assertTrue(AopUtils.isJdkDynamicProxy(proxy));

    try (Fixture fixture = fixture("classOnlyBean", JdkClassOnlyBean.class, () -> proxy, false)) {
      fixture.registration.register();

      assertTrue(fixture.submissionService.invocations.isEmpty());
      assertTrue(
          output
              .toString()
              .contains(
                  "Declare the method on a proxied interface or set"
                      + " spring.aop.proxy-target-class=true."));
    }
  }

  @Test
  void inheritedRecurringMethodIsDiscoveredAndInvoked() throws Exception {
    InheritedRecurringBean bean = new InheritedRecurringBean();
    try (Fixture fixture =
        fixture("inheritedBean", InheritedRecurringBean.class, () -> bean, false)) {
      fixture.registration.register();

      assertEquals(
          recurringInvocation(InheritedRecurringBean.class, "inherited", false),
          fixture.onlyInvocation());
      fixture.invokeOnlyInvocation();
      assertEquals(1, bean.invocations.get());
    }
  }

  @Test
  void beanFactoryMethodDefinedRecurringBeanIsDiscovered() {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.register(FactoryMethodConfiguration.class);

    try (Fixture fixture = fixture(context, className -> true)) {
      fixture.registration.register();

      assertEquals(
          recurringInvocation(FactoryRecurringBean.class, "run", false), fixture.onlyInvocation());
    }
  }

  @Test
  void repeatedRuntimeRegistrationReconcilesToOneStoredDefinition() {
    try (Fixture fixture =
        fixture(
            "idempotentBean", IdempotentRecurringBean.class, IdempotentRecurringBean::new, false)) {
      fixture.registration.register();
      fixture.registration.cancel();
      fixture.registration.register();

      assertEquals(2, fixture.submissionService.submissionCount);
      assertEquals(1, fixture.store.definitionCount());
      assertEquals(2, fixture.store.confirmationLookups);
      assertTrue(fixture.store.contains("spring-idempotent"));
    }
  }

  @Test
  void classPolicyDenialSkipsRegistration() {
    try (Fixture fixture =
        fixture(
            "deniedBean",
            DeniedRecurringBean.class,
            DeniedRecurringBean::new,
            false,
            className -> false)) {
      fixture.registration.register();

      assertTrue(fixture.submissionService.invocations.isEmpty());
      assertEquals(0, fixture.store.definitionCount());
    }
  }

  @Test
  void prototypeBeanIsDestroyedAfterSuccessfulInvocation() throws Exception {
    SuccessfulPrototypeBean.reset();
    try (Fixture fixture =
        fixture(
            "successfulPrototype",
            SuccessfulPrototypeBean.class,
            SuccessfulPrototypeBean::new,
            true)) {
      fixture.registration.register();
      int destroyedBeforeInvocation = SuccessfulPrototypeBean.destroyed.get();

      fixture.invokeOnlyInvocation();

      assertEquals(1, SuccessfulPrototypeBean.invocations.get());
      assertEquals(destroyedBeforeInvocation + 1, SuccessfulPrototypeBean.destroyed.get());
    }
  }

  @Test
  void prototypeBeanIsDestroyedAfterFailedInvocation() {
    FailingPrototypeBean.reset();
    try (Fixture fixture =
        fixture("failingPrototype", FailingPrototypeBean.class, FailingPrototypeBean::new, true)) {
      fixture.registration.register();
      int destroyedBeforeInvocation = FailingPrototypeBean.destroyed.get();

      assertThrows(IllegalStateException.class, fixture::invokeOnlyInvocation);

      assertEquals(1, FailingPrototypeBean.invocations.get());
      assertEquals(destroyedBeforeInvocation + 1, FailingPrototypeBean.destroyed.get());
    }
  }

  private static Fixture fixture(
      String beanName, Class<?> userClass, Supplier<?> supplier, boolean prototype) {
    return fixture(beanName, userClass, supplier, prototype, className -> true);
  }

  private static Fixture fixture(
      String beanName,
      Class<?> userClass,
      Supplier<?> supplier,
      boolean prototype,
      ClassPolicy classPolicy) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    RootBeanDefinition definition = new RootBeanDefinition(userClass);
    definition.setInstanceSupplier(supplier);
    if (prototype) {
      definition.setScope(RootBeanDefinition.SCOPE_PROTOTYPE);
    }
    context.registerBeanDefinition(beanName, definition);
    return fixture(context, classPolicy);
  }

  private static Fixture fixture(
      AnnotationConfigApplicationContext context, ClassPolicy classPolicy) {
    return new Fixture(context, classPolicy);
  }

  private static JobInvocation recurringInvocation(
      Class<?> beanClass, String methodName, boolean hasJobContextParam) {
    return new JobInvocation(
        RecurringMethodInvoker.class.getName(),
        "invoke",
        "(Ljava/lang/String;Ljava/lang/String;Z)V",
        false,
        List.of(beanClass.getName(), methodName, hasJobContextParam));
  }

  private static final class Fixture implements AutoCloseable {
    private final AnnotationConfigApplicationContext context;
    private final InMemoryRecurringStore store = new InMemoryRecurringStore();
    private final RecordingInvocationSubmissionService submissionService =
        new RecordingInvocationSubmissionService(store);
    private final RecurringMethodInvoker methodInvoker;
    private final RecurringMethodRegistrar registrar;
    private final RecurringRegistration registration;

    private Fixture(AnnotationConfigApplicationContext context, ClassPolicy classPolicy) {
      this.context = context;
      TestPropertyValues.of("ratchet.lifecycle.defer-auto-start=true").applyTo(context);
      context.register(RatchetAutoConfiguration.class);
      context.registerBean(
          ClassPolicy.class, () -> classPolicy, definition -> definition.setPrimary(true));
      context.registerBean(
          InvocationSubmissionService.class,
          submissionService::service,
          definition -> definition.setPrimary(true));
      context.registerBean(
          RecurringAnnotationMaintenanceService.class,
          () -> (registeredIds, nodeStartTime) -> 0,
          definition -> definition.setPrimary(true));
      context.registerBean(
          StartupCoordinator.class,
          () ->
              new StartupCoordinator() {
                @Override
                public boolean tryAcquire(String actionName, Duration leaseTtl) {
                  return true;
                }

                @Override
                public void release(String actionName) {}
              },
          definition -> definition.setPrimary(true));
      context.registerBean(
          ExecutorProvider.class,
          () ->
              new ExecutorProvider() {
                @Override
                public ExecutorService getJobExecutor() {
                  return null;
                }

                @Override
                public ScheduledExecutorService getScheduledExecutor() {
                  return null;
                }
              },
          definition -> definition.setPrimary(true));
      context.registerBean(
          JobStore.class, store::jobStore, definition -> definition.setPrimary(true));
      context.registerBean(
          RatchetOptions.class,
          RatchetOptions::defaults,
          definition -> definition.setPrimary(true));
      context.registerBean(
          Clock.class, Clock::systemUTC, definition -> definition.setPrimary(true));
      context.refresh();

      methodInvoker = context.getBean(RecurringMethodInvoker.class);
      registrar = context.getBean(RecurringMethodRegistrar.class);
      registration = context.getBean(RecurringRegistration.class);
    }

    private RecordedRecurringInvocation onlySubmission() {
      assertEquals(1, submissionService.invocations.size());
      return submissionService.invocations.get(0);
    }

    private JobInvocation onlyInvocation() {
      return onlySubmission().invocation;
    }

    private void invokeOnlyInvocation() throws Exception {
      JobInvocation invocation = onlyInvocation();
      assertEquals(RecurringMethodInvoker.class.getName(), invocation.targetClass());
      assertEquals("invoke", invocation.methodName());
      assertEquals("(Ljava/lang/String;Ljava/lang/String;Z)V", invocation.methodDescriptor());
      assertFalse(invocation.staticMethod());
      assertEquals(3, invocation.arguments().size());
      methodInvoker.invoke(
          (String) invocation.arguments().get(0),
          (String) invocation.arguments().get(1),
          (Boolean) invocation.arguments().get(2));
    }

    @Override
    public void close() {
      context.close();
    }
  }

  private static final class RecordingInvocationSubmissionService implements InvocationHandler {
    private final InMemoryRecurringStore store;
    private final List<RecordedRecurringInvocation> invocations = new ArrayList<>();
    private final Map<String, JobInvocation> definitions = new LinkedHashMap<>();
    private final InvocationSubmissionService service;
    private int submissionCount;

    private RecordingInvocationSubmissionService(InMemoryRecurringStore store) {
      this.store = store;
      this.service =
          (InvocationSubmissionService)
              Proxy.newProxyInstance(
                  getClass().getClassLoader(),
                  new Class<?>[] {InvocationSubmissionService.class},
                  this);
    }

    private InvocationSubmissionService service() {
      return service;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      if (method.getName().equals("scheduleRecurringInvocation")) {
        RecordedRecurringInvocation invocation =
            new RecordedRecurringInvocation(
                (String) args[0], (ZoneId) args[1], (JobInvocation) args[2]);
        invocations.add(invocation);
        return new RecordingRecurringInvocationBuilder(this, invocation);
      }
      throw new UnsupportedOperationException("Unexpected submission call: " + method);
    }

    private JobHandle submit(RecordedRecurringInvocation invocation) {
      submissionCount++;
      definitions.put(invocation.businessKey, invocation.invocation);
      UUID id = store.reconcile(invocation.businessKey);
      return () -> id;
    }
  }

  private static final class RecordedRecurringInvocation {
    private final String cron;
    private final ZoneId zone;
    private final JobInvocation invocation;
    private String businessKey;

    private RecordedRecurringInvocation(String cron, ZoneId zone, JobInvocation invocation) {
      this.cron = cron;
      this.zone = zone;
      this.invocation = invocation;
    }
  }

  private static final class RecordingRecurringInvocationBuilder implements RecurringJobBuilder {
    private final RecordingInvocationSubmissionService submissionService;
    private final RecordedRecurringInvocation invocation;

    private RecordingRecurringInvocationBuilder(
        RecordingInvocationSubmissionService submissionService,
        RecordedRecurringInvocation invocation) {
      this.submissionService = submissionService;
      this.invocation = invocation;
    }

    @Override
    public RecurringJobBuilder withOptions(JobOptions options) {
      return this;
    }

    @Override
    public RecurringJobBuilder withTags(List<String> tags) {
      return this;
    }

    @Override
    public RecurringJobBuilder withBusinessKey(String key) {
      invocation.businessKey = key;
      return this;
    }

    @Override
    public RecurringJobBuilder withMisfirePolicy(RecurringMisfirePolicy policy) {
      return this;
    }

    @Override
    public RecurringJobBuilder virtual() {
      return this;
    }

    @Override
    public RecurringJobBuilder platform() {
      return this;
    }

    @Override
    public RecurringJobBuilder withEncryptedPayload() {
      return this;
    }

    @Override
    public JobHandle submit() {
      return submissionService.submit(invocation);
    }
  }

  private static final class InMemoryRecurringStore implements InvocationHandler {
    private final Map<String, UUID> definitions = new LinkedHashMap<>();
    private final JobStore jobStore;
    private int confirmationLookups;

    private InMemoryRecurringStore() {
      jobStore =
          (JobStore)
              Proxy.newProxyInstance(
                  getClass().getClassLoader(),
                  new Class<?>[] {JobStore.class, RecurringJobStore.class},
                  this);
    }

    private JobStore jobStore() {
      return jobStore;
    }

    private UUID reconcile(String businessKey) {
      return definitions.computeIfAbsent(businessKey, ignored -> UUID.randomUUID());
    }

    private int definitionCount() {
      return definitions.size();
    }

    private boolean contains(String businessKey) {
      return definitions.containsKey(businessKey);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      if (method.getName().equals("capability")) {
        Class<?> capability = (Class<?>) args[0];
        return capability == RecurringJobStore.class
            ? Optional.of(capability.cast(proxy))
            : Optional.empty();
      }
      if (method.getName().equals("findRecurringByBusinessKey")) {
        confirmationLookups++;
        String businessKey = (String) args[0];
        UUID id = definitions.get(businessKey);
        return id == null ? Optional.empty() : Optional.of(recurringDefinition(id, businessKey));
      }
      throw new UnsupportedOperationException("Unexpected store call: " + method);
    }

    private static RecurringJobDefinition recurringDefinition(UUID id, String businessKey) {
      return new RecurringJobDefinition(
          id,
          "0 0/5 * * * ?",
          "UTC",
          Instant.EPOCH,
          false,
          null,
          5,
          0,
          null,
          0,
          0,
          null,
          null,
          null,
          businessKey,
          null,
          null,
          Instant.EPOCH,
          null,
          false,
          RecurringMisfirePolicy.skip());
    }
  }

  private static Object objectMethod(Object proxy, Method method, Object[] args) {
    return switch (method.getName()) {
      case "equals" -> proxy == args[0];
      case "hashCode" -> System.identityHashCode(proxy);
      case "toString" -> proxy.getClass().getInterfaces()[0].getSimpleName() + " test double";
      default -> throw new UnsupportedOperationException("Unexpected Object method: " + method);
    };
  }

  public static class CglibRecurringBean {
    private final AtomicInteger invocations = new AtomicInteger();

    @Recurring(id = "spring-cglib", cron = "0 0/5 * * * ?")
    public void run() {
      invocations.incrementAndGet();
    }
  }

  public interface JdkRecurringContract {
    void run();
  }

  public static class JdkRecurringBean implements JdkRecurringContract {
    private final AtomicInteger invocations = new AtomicInteger();

    @Override
    @Recurring(id = "spring-jdk-interface", cron = "0 0/5 * * * ?")
    public void run() {
      invocations.incrementAndGet();
    }
  }

  public interface MarkerContract {}

  public static class JdkClassOnlyBean implements MarkerContract {

    @Recurring(id = "spring-jdk-class-only", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  public static class RecurringBase {
    protected final AtomicInteger invocations = new AtomicInteger();

    @Recurring(id = "spring-inherited", cron = "0 0/5 * * * ?")
    public void inherited() {
      invocations.incrementAndGet();
    }
  }

  public static class InheritedRecurringBean extends RecurringBase {}

  public static class FactoryRecurringBean {

    @Recurring(id = "spring-factory-method", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  @Configuration(proxyBeanMethods = false)
  static class FactoryMethodConfiguration {

    @Bean
    FactoryRecurringBean factoryRecurringBean() {
      return new FactoryRecurringBean();
    }
  }

  public static class IdempotentRecurringBean {

    @Recurring(id = "spring-idempotent", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  public static class DeniedRecurringBean {

    @Recurring(id = "spring-denied", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  public static class SuccessfulPrototypeBean implements DisposableBean {
    private static final AtomicInteger invocations = new AtomicInteger();
    private static final AtomicInteger destroyed = new AtomicInteger();

    static void reset() {
      invocations.set(0);
      destroyed.set(0);
    }

    @Recurring(id = "spring-prototype-success", cron = "0 0/5 * * * ?")
    public void run() {
      invocations.incrementAndGet();
    }

    @Override
    public void destroy() {
      destroyed.incrementAndGet();
    }
  }

  public static class FailingPrototypeBean implements DisposableBean {
    private static final AtomicInteger invocations = new AtomicInteger();
    private static final AtomicInteger destroyed = new AtomicInteger();

    static void reset() {
      invocations.set(0);
      destroyed.set(0);
    }

    @Recurring(id = "spring-prototype-failure", cron = "0 0/5 * * * ?")
    public void run() {
      invocations.incrementAndGet();
      throw new IllegalStateException("prototype invocation failed");
    }

    @Override
    public void destroy() {
      destroyed.incrementAndGet();
    }
  }
}
