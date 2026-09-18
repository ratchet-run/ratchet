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
package run.ratchet.consumer.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import example.ratchet.ConsumerApplication;
import example.ratchet.ConsumerJobs;
import example.ratchet.ConsumerRecord;
import example.ratchet.ConsumerService;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.persistence.EntityManagerFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.Recurring;
import run.ratchet.api.event.JobPausedEvent;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.migration.SchemaInitializationException;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/** Real consumer contexts: the fixtures stay outside the application's component scan. */
class SqlLifecycleTest {
  private static SqlDatabase database;
  private static final AtomicInteger EXECUTED = new AtomicInteger();
  private static final AtomicInteger DESTROYED = new AtomicInteger();
  private static final AtomicInteger CREATED = new AtomicInteger();
  private static final AtomicInteger HOOK_STOPPED = new AtomicInteger();
  private static final AtomicInteger HOOK_DESTROYED = new AtomicInteger();
  private static final List<String> ORDER = new CopyOnWriteArrayList<>();

  @BeforeAll
  static void database() {
    database = SqlDatabase.start();
  }

  @AfterAll
  static void stopDatabase() {
    database.close();
  }

  @BeforeEach
  void counters() {
    EXECUTED.set(0);
    DESTROYED.set(0);
    CREATED.set(0);
    HOOK_STOPPED.set(0);
    HOOK_DESTROYED.set(0);
    ORDER.clear();
  }

  @AfterEach
  void noOwnedThreadsRemain() {
    await()
        .atMost(Duration.ofSeconds(12))
        .untilAsserted(
            () ->
                assertThat(
                        Thread.getAllStackTraces().keySet().stream()
                            .filter(Thread::isAlive)
                            .map(Thread::getName)
                            .filter(n -> n.startsWith("ratchet-standalone-"))
                            .toList())
                    .isEmpty());
  }

  @Test
  void sequentialContextsReleaseConfigurationAndExecuteAgain() {
    PayloadSerializer baseline = PayloadSerializerHolder.get();
    try (var first = start()) {
      assertExecution(first, "first");
    }
    assertThat(PayloadSerializerHolder.get()).isSameAs(baseline);
    try (var second = start()) {
      assertExecution(second, "second");
    }
    assertThat(PayloadSerializerHolder.get()).isSameAs(baseline);
  }

  @Test
  void mandatoryOnlyStoreExecutesOrdinaryJobsAndRejectsUnsupportedSubmissionsWithoutRows() {
    try (var context = start(MandatoryOnlyStore.class)) {
      JobStore store = context.getBean(JobStore.class);
      for (Class<?> capability :
          List.of(
              ArchiveStore.class,
              BatchStore.class,
              JobAnalyticsStore.class,
              JobAuditStore.class,
              JobExtensionStore.class,
              JobQueryStore.class,
              LockStore.class,
              RecurringJobStore.class,
              ResourcePermitStore.class,
              SignalStore.class,
              WorkflowConditionStore.class)) {
        assertThat(capability.isInstance(store)).isFalse();
        assertThat(store.capability(capability)).isEmpty();
      }
      assertExecution(context, "mandatory-only");
      JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
      Long jobCount = jdbc.queryForObject("select count(*) from scheduler_job", Long.class);
      Long recurringCount =
          jdbc.queryForObject("select count(*) from scheduler_recurring_job", Long.class);
      Long batchCount = jdbc.queryForObject("select count(*) from scheduler_batch", Long.class);
      var submission = context.getBean(InvocationSubmissionService.class);
      var invocation =
          new JobInvocation(
              ConsumerJobs.class.getName(),
              "complete",
              "(Ljava/lang/String;)V",
              false,
              List.of("unsupported"));

      assertThatThrownBy(
              () ->
                  submission
                      .enqueueInvocationBatch("unsupported")
                      .forEach(List.of("child"), ignored -> invocation)
                      .submit())
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("BatchStore");
      assertThatThrownBy(
              () ->
                  submission
                      .scheduleRecurringInvocation("0 0 0 * * ?", ZoneOffset.UTC, invocation)
                      .withBusinessKey("mandatory-only-recurring")
                      .submit())
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("RecurringJobStore");
      assertThatThrownBy(
              () ->
                  submission
                      .enqueueInvocation(invocation)
                      .withResource("unsupported")
                      .immediate()
                      .submit())
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("ResourcePermitStore");

      assertThat(jdbc.queryForObject("select count(*) from scheduler_job", Long.class))
          .isEqualTo(jobCount);
      assertThat(jdbc.queryForObject("select count(*) from scheduler_recurring_job", Long.class))
          .isEqualTo(recurringCount);
      assertThat(jdbc.queryForObject("select count(*) from scheduler_batch", Long.class))
          .isEqualTo(batchCount);
      assertThat(
              context.getBean(JobSchedulerService.class).cancelRecurringJobByBusinessKey("absent"))
          .isZero();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class MandatoryOnlyStore {
    @Bean
    static BeanPostProcessor mandatoryStoreFacade() {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String name) {
          if (!(bean instanceof JobStore)) return bean;
          assertThat(AopUtils.isAopProxy(bean)).as("underlying SQL transaction proxy").isTrue();
          return Proxy.newProxyInstance(
              JobStore.class.getClassLoader(),
              new Class<?>[] {JobStore.class},
              (proxy, method, arguments) -> {
                if (method.getName().equals("capability")) {
                  return InvocationHandler.invokeDefault(proxy, method, arguments);
                }
                try {
                  return method.invoke(bean, arguments);
                } catch (InvocationTargetException failure) {
                  throw failure.getCause();
                }
              });
        }
      };
    }
  }

  @Test
  void conflictingSecondContextCannotReplaceFirstConfigurationOrStopItsWorkers() {
    try (var first = start()) {
      PayloadSerializer serializer = first.getBean(PayloadSerializer.class);
      assertThatThrownBy(() -> start())
          .hasStackTraceContaining("already owns converter configuration");
      assertThat(PayloadSerializerHolder.get()).isSameAs(serializer);
      assertExecution(first, "survives-rejected-context");
    }
  }

  @Test
  void prototypeInvocationUsesJdkAdviceAndReleasesEveryAcquiredTarget() {
    try (var context = startWith(false, PrototypeJobs.class)) {
      int createdBefore = CREATED.get();
      int destroyedBefore = DESTROYED.get();
      var submissions = context.getBean(InvocationSubmissionService.class);
      for (int i = 0; i < 2; i++) {
        submissions
            .enqueueInvocation(
                new JobInvocation(PrototypeTask.class.getName(), "run", "()V", false, List.of()))
            .immediate()
            .submit();
      }
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(EXECUTED).hasValue(2);
                assertThat(CREATED.get() - createdBefore).isEqualTo(2);
                assertThat(DESTROYED.get() - destroyedBefore).isEqualTo(2);
              });
    }
  }

  @Test
  void singletonClassProxyKeepsAdviceAndLivesUntilApplicationShutdown() {
    try (var context = startWith(true, SingletonJobs.class)) {
      assertThat(AopUtils.isCglibProxy(context.getBean(SingletonTask.class))).isTrue();
      context
          .getBean(InvocationSubmissionService.class)
          .enqueueInvocation(
              new JobInvocation(SingletonTask.class.getName(), "run", "()V", false, List.of()))
          .immediate()
          .submit();
      await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(EXECUTED).hasValue(1));
      assertThat(DESTROYED).hasValue(0);
    }
    assertThat(DESTROYED).hasValue(1);
  }

  @Test
  void exposedJdkRecurringMethodRegistersBeforeRunnerAndRunsWithTransactionAdvice() {
    try (var context = startWith(false, RecurringJobs.class, StartupOrder.class)) {
      assertThat(AopUtils.isJdkDynamicProxy(context.getBean("recurringTask"))).isTrue();
      assertThat(ORDER)
          .containsExactly("singletons-ready", "before-start", "after-start", "runner");
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(EXECUTED.get()).isGreaterThan(0));
    }
  }

  @Test
  void unexposedJdkRecurringMethodFailsStartupAndReleasesRuntimeOwnership() {
    assertThatThrownBy(() -> startWith(false, UnexposedRecurringJobs.class))
        .hasStackTraceContaining("cannot invoke @Recurring method through its Spring proxy");
    try (var recovered = start()) {
      assertExecution(recovered, "after-recurring-failure");
    }
  }

  @Test
  void fatalStartupHookFailureSkipsStopCallbacksAndDestroysPrototypeExactlyOnce() {
    assertThatThrownBy(() -> startWith(true, FailingHooks.class))
        .hasStackTraceContaining("intentional lifecycle startup failure");
    assertThat(HOOK_STOPPED).hasValue(0);
    assertThat(HOOK_DESTROYED).hasValue(1);
    try (var recovered = start()) {
      assertExecution(recovered, "after-hook-failure");
    }
  }

  @Test
  void unavailableCustomScheduledExecutorFailsStartupAndLeavesNoOwnedRuntime() {
    assertThatThrownBy(() -> startWith(true, BrokenExecutor.class))
        .hasStackTraceContaining("intentional executor unavailable");
    try (var recovered = start()) {
      assertExecution(recovered, "after-executor-failure");
    }
  }

  @Test
  void customExecutorProviderIsUsedAndItsPoolsRemainOwnedByApplication() {
    ApplicationExecutors executors;
    try (var context = startWith(true, CustomExecutor.class)) {
      executors = context.getBean(ApplicationExecutors.class);
      assertThat(context.containsBean("ratchetExecutorProvider")).isFalse();
      assertExecution(context, "custom-executor");
    }
    try {
      assertThat(executors.workers.isShutdown()).isFalse();
      assertThat(executors.scheduler.isShutdown()).isFalse();
    } finally {
      executors.workers.shutdownNow();
      executors.scheduler.shutdownNow();
    }
  }

  @Test
  void shutdownDrainsRunningWorkBeforeReturningAndLeavesApplicationExecutorsOpen()
      throws Exception {
    BlockingTask.reset();
    var context = startWith(true, CustomExecutor.class, BlockingJobs.class);
    ApplicationExecutors executors = context.getBean(ApplicationExecutors.class);
    try {
      enqueueBlocking(context);
      assertThat(BlockingTask.entered.await(20, TimeUnit.SECONDS)).isTrue();
      DrainController drain = context.getBean(DrainController.class);
      CompletableFuture<Void> closing = CompletableFuture.runAsync(context::close);
      await().atMost(Duration.ofSeconds(5)).until(drain::isDraining);
      assertThat(closing.isDone()).isFalse();
      assertThat(BlockingTask.interruptions.get()).isZero();
      BlockingTask.release.countDown();
      closing.get(15, TimeUnit.SECONDS);
      assertThat(BlockingTask.interruptions.get()).isZero();
      assertThat(executors.workers.isShutdown()).isFalse();
      assertThat(executors.scheduler.isShutdown()).isFalse();
    } finally {
      BlockingTask.release.countDown();
      context.close();
      executors.workers.shutdownNow();
      executors.scheduler.shutdownNow();
    }
  }

  @Test
  void interruptIgnoringExecutionFencesNewRuntimeUntilRunnerActuallyExits() throws Exception {
    PayloadSerializer original = PayloadSerializerHolder.get();
    BlockingTask.reset();
    var context =
        new SpringApplicationBuilder(
                ConsumerApplication.class, CustomExecutor.class, BlockingJobs.class)
            .properties(properties(true))
            .run("--ratchet.shutdown-timeout=0s");
    ApplicationExecutors executors = context.getBean(ApplicationExecutors.class);
    JobExecutorService execution = context.getBean(JobExecutorService.class);
    try {
      enqueueBlocking(context);
      assertThat(BlockingTask.entered.await(20, TimeUnit.SECONDS)).isTrue();
      CompletableFuture.runAsync(context::close).get(15, TimeUnit.SECONDS);
      assertThat(BlockingTask.interruptions.get()).isPositive();
      assertThat(execution.awaitIdle(Duration.ZERO)).isFalse();
      assertThatThrownBy(() -> start())
          .hasStackTraceContaining("already owns converter configuration");
      BlockingTask.release.countDown();
      assertThat(execution.awaitIdle(Duration.ofSeconds(10))).isTrue();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(PayloadSerializerHolder.get()).isSameAs(original));
      try (var next = start()) {
        assertExecution(next, "after-interrupt-ignoring-runner");
      }
    } finally {
      BlockingTask.release.countDown();
      execution.awaitIdle(Duration.ofSeconds(10));
      context.close();
      executors.workers.shutdownNow();
      executors.scheduler.shutdownNow();
    }
  }

  private static void enqueueBlocking(ConfigurableApplicationContext context) {
    context
        .getBean(InvocationSubmissionService.class)
        .enqueueInvocation(
            new JobInvocation(BlockingTask.class.getName(), "run", "()V", false, List.of()))
        .immediate()
        .submit();
  }

  public static class BlockingTask {
    static CountDownLatch entered;
    static CountDownLatch release;
    static final AtomicInteger interruptions = new AtomicInteger();

    static void reset() {
      entered = new CountDownLatch(1);
      release = new CountDownLatch(1);
      interruptions.set(0);
    }

    public void run() {
      entered.countDown();
      while (true) {
        try {
          release.await();
          return;
        } catch (InterruptedException ignored) {
          interruptions.incrementAndGet();
        }
      }
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class BlockingJobs {
    @Bean
    BlockingTask blockingTask() {
      return new BlockingTask();
    }
  }

  @Test
  void disabledIntegrationCreatesNoRatchetSchemaDiscoveryOrRuntime() throws Exception {
    try (var isolated = SqlDatabase.startIsolated()) {
      var props = properties(true);
      props.putAll(isolated.properties());
      props.put("ratchet.enabled", "false");
      try (var context =
          new SpringApplicationBuilder(DisabledApplication.class, UnexposedRecurringJobs.class)
              .properties(props)
              .run("--ratchet.enabled=false")) {
        var emf = context.getBean(EntityManagerFactory.class);
        assertThat(context.getEnvironment().getProperty("spring.jpa.hibernate.ddl-auto"))
            .isEqualTo("validate");
        assertThat(emf.getMetamodel().getEntities())
            .extracting(type -> type.getJavaType().getName())
            .containsExactly(ConsumerRecord.class.getName());
        context
            .getBean(JdbcTemplate.class)
            .update(
                "insert into consumer_record (id, state) values (?, ?)",
                "disabled-host",
                "preserved");
        try (var entityManager = emf.createEntityManager()) {
          assertThat(entityManager.find(ConsumerRecord.class, "disabled-host").getState())
              .isEqualTo("preserved");
        }
        assertThat(context.getBeansOfType(JobSchedulerService.class)).isEmpty();
        assertThat(context.containsBean("ratchetRuntimeInstallation")).isFalse();
        assertThat(context.containsBean("springRecurringDiscovery")).isFalse();
        assertThat(context.containsBean("ratchetRuntime")).isFalse();
        List<String> schedulerTables =
            context
                .getBean(JdbcTemplate.class)
                .execute(
                    (ConnectionCallback<List<String>>)
                        connection -> {
                          List<String> found = new ArrayList<>();
                          try (var tables =
                              connection
                                  .getMetaData()
                                  .getTables(
                                      connection.getCatalog(),
                                      connection.getSchema(),
                                      "%",
                                      new String[] {"TABLE"})) {
                            while (tables.next()) {
                              String table =
                                  tables.getString("TABLE_NAME").toLowerCase(Locale.ROOT);
                              if (table.startsWith("scheduler_")
                                  || table.equals("ratchet_schema_version")) found.add(table);
                            }
                          }
                          return found;
                        });
        assertThat(schedulerTables).isEmpty();
      }
    }
  }

  @Test
  void requiredSpringEventListenerSubmitsDurableWorkOnlyAfterCommit() {
    try (var context = start(RequiredEventListener.class)) {
      assertPausedListenerSubmission(context, "required-event-", true);
      assertPausedListenerSubmission(context, "required-event-", false);
    }
  }

  @Test
  void programmaticEventListenerCanCallRequiredSubmissionAfterCommit() {
    try (var context = start()) {
      ConsumerService service = context.getBean(ConsumerService.class);
      context
          .getBean(JobSchedulerService.class)
          .addEventListener(
              event -> {
                if (event instanceof JobPausedEvent paused
                    && paused.getBusinessKey() != null
                    && paused.getBusinessKey().startsWith("program-event-")) {
                  if (TransactionSynchronizationManager.isActualTransactionActive())
                    throw new IllegalStateException(
                        "completed transaction leaked into event listener");
                  service.submit("child-" + paused.getBusinessKey());
                }
              });
      assertPausedListenerSubmission(context, "program-event-", true);
      assertPausedListenerSubmission(context, "program-event-", false);
    }
  }

  private static void assertPausedListenerSubmission(
      ConfigurableApplicationContext context, String prefix, boolean commit) {
    String key = prefix + System.nanoTime();
    var parent =
        context
            .getBean(InvocationSubmissionService.class)
            .scheduleInvocation(
                Duration.ofHours(1),
                new JobInvocation(
                    ConsumerJobs.class.getName(),
                    "complete",
                    "(Ljava/lang/String;)V",
                    false,
                    List.of("unused")))
            .withBusinessKey(key)
            .submit();
    new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
        .executeWithoutResult(
            status -> {
              assertThat(context.getBean(JobSchedulerService.class).pauseJob(parent.id())).isTrue();
              assertThat(
                      context
                          .getBean(JdbcTemplate.class)
                          .queryForObject(
                              "select count(*) from consumer_record where id = ?",
                              Integer.class,
                              "child-" + key))
                  .isZero();
              if (!commit) status.setRollbackOnly();
            });
    if (commit) {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(
                          context
                              .getBean(JdbcTemplate.class)
                              .queryForObject(
                                  "select state from consumer_record where id = ?",
                                  String.class,
                                  "child-" + key))
                      .isEqualTo("executed"));
    } else {
      assertThat(
              context
                  .getBean(JdbcTemplate.class)
                  .queryForObject(
                      "select count(*) from consumer_record where id = ?",
                      Integer.class,
                      "child-" + key))
          .isZero();
    }
  }

  private static ConfigurableApplicationContext start(Class<?>... configs) {
    return startWith(true, configs);
  }

  private static ConfigurableApplicationContext startWith(
      boolean classProxies, Class<?>... configs) {
    var sources = new ArrayList<Class<?>>();
    sources.add(ConsumerApplication.class);
    sources.addAll(List.of(configs));
    return new SpringApplicationBuilder(sources.toArray(Class<?>[]::new))
        .properties(properties(classProxies))
        .run();
  }

  private static Map<String, Object> properties(boolean classProxies) {
    var properties = new HashMap<String, Object>(database.properties());
    properties.put("spring.aop.proxy-target-class", String.valueOf(classProxies));
    properties.put("ratchet.allowed-packages", "example.ratchet,run.ratchet.consumer.sql");
    properties.put("spring.main.banner-mode", "off");
    return properties;
  }

  private static void assertExecution(ConfigurableApplicationContext context, String prefix) {
    String id = prefix + '-' + System.nanoTime();
    context.getBean(ConsumerService.class).submit(id);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        context
                            .getBean(JdbcTemplate.class)
                            .queryForObject(
                                "select state from consumer_record where id = ?", String.class, id))
                    .isEqualTo("executed"));
  }

  public interface Task {
    void run();
  }

  public static class PrototypeTask implements Task {
    public PrototypeTask() {
      CREATED.incrementAndGet();
    }

    @Override
    @Transactional
    public void run() {
      if (!TransactionSynchronizationManager.isActualTransactionActive())
        throw new IllegalStateException("proxy advice bypassed");
      EXECUTED.incrementAndGet();
    }

    @PreDestroy
    public void destroy() {
      DESTROYED.incrementAndGet();
    }
  }

  public static class SingletonTask extends PrototypeTask {}

  public static class RecurringTask implements Task {
    @Override
    @Transactional
    @Recurring(cron = "0/1 * * * * ?", id = "lifecycle-exposed-recurring")
    public void run() {
      if (!TransactionSynchronizationManager.isActualTransactionActive())
        throw new IllegalStateException("recurring proxy advice bypassed");
      EXECUTED.incrementAndGet();
    }
  }

  public static class UnexposedTask implements Task {
    @Override
    @Transactional
    public void run() {}

    @Recurring(cron = "0/1 * * * * ?", id = "lifecycle-unexposed-recurring")
    public void unavailable() {}
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class PrototypeJobs {
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    PrototypeTask prototypeTask() {
      return new PrototypeTask();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class SingletonJobs {
    @Bean
    SingletonTask singletonTask() {
      return new SingletonTask();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class RecurringJobs {
    @Bean
    RecurringTask recurringTask() {
      return new RecurringTask();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class UnexposedRecurringJobs {
    @Bean
    UnexposedTask unexposedTask() {
      return new UnexposedTask();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class StartupOrder {
    @Bean
    SmartInitializingSingleton initializedSingletons() {
      return () -> ORDER.add("singletons-ready");
    }

    @Bean
    SchedulerLifecycleHook orderHook() {
      return new SchedulerLifecycleHook() {
        @Override
        public void beforeStart() {
          assertThat(ORDER).containsExactly("singletons-ready");
          ORDER.add("before-start");
        }

        @Override
        public void afterStart() {
          ORDER.add("after-start");
        }
      };
    }

    @Bean
    ApplicationRunner orderRunner(JdbcTemplate jdbc) {
      return args -> {
        assertThat(
                jdbc.queryForObject(
                    "select count(*) from scheduler_recurring_job where business_key ="
                        + " 'lifecycle-exposed-recurring'",
                    Integer.class))
            .isEqualTo(1);
        ORDER.add("runner");
      };
    }
  }

  @Priority(1)
  public static class CompletedHook implements SchedulerLifecycleHook {
    @Override
    public void afterStop() {
      HOOK_STOPPED.incrementAndGet();
    }

    @PreDestroy
    public void destroyed() {
      HOOK_DESTROYED.incrementAndGet();
    }
  }

  @Priority(2)
  public static class FailedHook implements SchedulerLifecycleHook {
    @Override
    public void beforeStart() {
      throw new SchemaInitializationException("intentional lifecycle startup failure");
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class FailingHooks {
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    CompletedHook completedHook() {
      return new CompletedHook();
    }

    @Bean
    FailedHook failedHook() {
      return new FailedHook();
    }
  }

  public static class ApplicationExecutors implements ExecutorProvider {
    final ExecutorService workers = Executors.newCachedThreadPool();
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    @Override
    public ExecutorService getJobExecutor() {
      return workers;
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
      return scheduler;
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class CustomExecutor {
    @Bean(destroyMethod = "")
    ApplicationExecutors customExecutorProvider() {
      return new ApplicationExecutors();
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class BrokenExecutor {
    @Bean
    ExecutorProvider brokenExecutorProvider() {
      return new ExecutorProvider() {
        @Override
        public ExecutorService getJobExecutor() {
          throw new IllegalStateException("intentional executor unavailable");
        }

        @Override
        public ScheduledExecutorService getScheduledExecutor() {
          throw new IllegalStateException("intentional executor unavailable");
        }
      };
    }
  }

  public static class RequiredPausedListener {
    private final ConsumerService service;

    public RequiredPausedListener(ConsumerService service) {
      this.service = service;
    }

    @EventListener
    @Transactional
    public void onPaused(JobPausedEvent event) {
      if (event.getBusinessKey() != null && event.getBusinessKey().startsWith("required-event-")) {
        if (!TransactionSynchronizationManager.isActualTransactionActive())
          throw new IllegalStateException("event listener advice missing");
        service.submit("child-" + event.getBusinessKey());
      }
    }
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class RequiredEventListener {
    @Bean
    RequiredPausedListener requiredPausedListener(ConsumerService service) {
      return new RequiredPausedListener(service);
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class DisabledApplication {
    @Bean
    PersistenceManagedTypes applicationEntities() {
      return PersistenceManagedTypes.of(ConsumerRecord.class.getName());
    }
  }
}
