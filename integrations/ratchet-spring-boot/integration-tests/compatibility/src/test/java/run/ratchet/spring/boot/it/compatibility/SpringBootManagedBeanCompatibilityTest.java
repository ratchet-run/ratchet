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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.AopTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.BatchRecoveryService;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.runtime.RatchetRuntimeComponentCatalog;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(OutputCaptureExtension.class)
class SpringBootManagedBeanCompatibilityTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
          .withPropertyValues("ratchet.allow-empty-class-policy=true")
          .withUserConfiguration(ManagedApplication.class);

  @Test
  void fullRuntimeStartsWithScheduledBackgroundServicesEnabled(CapturedOutput output) {
    fullGraphRunner(true)
        .run(
            context -> {
              assertTrue(context.getBean("ratchetLifecycle", SmartLifecycle.class).isRunning());
              String startupOutput = output.getAll();
              assertTrue(startupOutput.contains("DefaultPollerScheduler started"));
              assertFalse(
                  startupOutput.contains(
                      "Managed scheduled executor unavailable during Ratchet startup"));
            });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void cataloguedComponentsAreAdvisedAndPropagateThroughInjectedReferences(
      boolean proxyTargetClass) {
    contextRunner
        .withPropertyValues("spring.aop.proxy-target-class=" + proxyTargetClass)
        .run(
            context -> {
              JobStateManager jobStateManager = context.getBean(JobStateManager.class);
              DeadLetterService deadLetterService = context.getBean(DeadLetterService.class);
              PostExecutionHandler postExecutionHandler =
                  context.getBean(PostExecutionHandler.class);
              RecordingTransactionManager transactionManager =
                  context.getBean(RecordingTransactionManager.class);

              assertTrue(AopUtils.isAopProxy(jobStateManager));
              assertTrue(AopUtils.isAopProxy(deadLetterService));
              assertTrue(AopUtils.isAopProxy(postExecutionHandler));

              jobStateManager.resetRunningJobsForNode();
              assertEquals(List.of("BEGIN(REQUIRED)", "COMMIT"), transactionManager.events());

              DeadLetterService injectedReference = injectedDeadLetterService(postExecutionHandler);
              assertSame(deadLetterService, injectedReference);
              assertTrue(AopUtils.isAopProxy(injectedReference));

              transactionManager.clear();
              JobEntity job = mock(JobEntity.class);
              when(job.getId()).thenReturn(UUID.randomUUID());
              postExecutionHandler.moveToDlq(job, new IllegalStateException("failure"));

              assertEquals(
                  List.of(
                      "BEGIN(REQUIRES_NEW)",
                      "SUSPEND",
                      "BEGIN(REQUIRES_NEW)",
                      "COMMIT",
                      "RESUME",
                      "COMMIT"),
                  transactionManager.events());
            });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void cataloguedSelfInvocationDoesNotReenterTheTransactionProxy(boolean proxyTargetClass) {
    contextRunner
        .withUserConfiguration(SelfInvocationOverrides.class)
        .withPropertyValues("spring.aop.proxy-target-class=" + proxyTargetClass)
        .run(
            context -> {
              PostExecutionHandler postExecutionHandler =
                  context.getBean(PostExecutionHandler.class);
              RecordingTransactionManager transactionManager =
                  context.getBean(RecordingTransactionManager.class);
              JobEntity job = mock(JobEntity.class);
              when(job.getJobType()).thenReturn(JobExecutionType.SINGLE);
              IllegalStateException failure = new IllegalStateException("failure");

              postExecutionHandler.moveToDlq(job, failure);
              assertEquals(List.of("BEGIN(REQUIRES_NEW)", "COMMIT"), transactionManager.events());

              transactionManager.clear();
              postExecutionHandler.handlePermanentFailure(job, failure);
              assertEquals(List.of("BEGIN(REQUIRES_NEW)", "COMMIT"), transactionManager.events());
            });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void everyCataloguedTransactionalComponentIsManagedAndAdvised(boolean proxyTargetClass) {
    fullGraphRunner(proxyTargetClass)
        .run(
            context -> {
              RatchetRuntimeComponentCatalog.components().stream()
                  .filter(descriptor -> descriptor.transactional())
                  .forEach(
                      descriptor -> {
                        Object bean = context.getBean(descriptor.componentType().getName());
                        assertTrue(
                            AopUtils.isAopProxy(bean),
                            () -> descriptor.componentType().getName() + " is not advised");
                      });

              BatchRecoveryService recoveryService = context.getBean(BatchRecoveryService.class);
              BatchRecoveryService target = AopTestUtils.getTargetObject(recoveryService);
              assertSame(
                  context.getBean(BatchStore.class),
                  field(target, BatchRecoveryService.class, "batchStore"),
                  "The catalog constructor must win over the classpath-visible @Inject constructor");
            });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void batchRecoveryUsesRequiresNewWhileOrdinaryChildCompletionUsesRequired(
      boolean proxyTargetClass) {
    fullGraphRunner(proxyTargetClass)
        .run(
            context -> {
              UUID batchId = UUID.randomUUID();
              BatchEntity batch = new BatchEntity();
              batch.setId(batchId);
              JobEntity parent = new JobEntity();
              parent.setId(batchId);
              parent.setStatus(JobStatus.PENDING);
              BatchStore batchStore = context.getBean(BatchStore.class);
              JobCrudStore jobCrudStore = context.getBean(JobCrudStore.class);
              when(batchStore.findRecoverableBatchIds(100)).thenReturn(List.of(batchId));
              when(batchStore.findBatchesByIds(List.of(batchId))).thenReturn(List.of(batch));
              when(jobCrudStore.findByIds(List.of(batchId))).thenReturn(List.of(parent));
              when(batchStore.markBatchCompleteIfReady(batchId)).thenReturn(false);

              BatchService batchService = context.getBean(BatchService.class);
              RecordingTransactionManager transactionManager =
                  context.getBean(RecordingTransactionManager.class);
              transactionManager.clear();

              assertEquals(0, batchService.recoverStuckBatches());
              assertEquals(List.of("BEGIN(REQUIRES_NEW)", "COMMIT"), transactionManager.events());

              transactionManager.clear();
              assertFalse(batchService.markChildSucceeded(new JobEntity()));
              assertEquals(List.of("BEGIN(REQUIRED)", "COMMIT"), transactionManager.events());
            });
  }

  private static ApplicationContextRunner fullGraphRunner(boolean proxyTargetClass) {
    return new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
        .withInitializer(context -> registerStoreFixtures((GenericApplicationContext) context))
        .withUserConfiguration(FullGraphApplication.class)
        .withPropertyValues(
            "ratchet.class-policy.allowed-packages=run.ratchet",
            "spring.aop.proxy-target-class=" + proxyTargetClass);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerStoreFixtures(GenericApplicationContext context) {
    context.registerBean("fullGraphJobStore", JobStore.class, () -> mock(JobStore.class));
    Set<Class<?>> storeCapabilities = new LinkedHashSet<>();
    RatchetRuntimeComponentCatalog.components().stream()
        .flatMap(descriptor -> descriptor.constructorParameterTypes().stream())
        .filter(type -> type.getPackageName().equals("run.ratchet.store.spi"))
        .filter(type -> !type.isAssignableFrom(JobStore.class))
        .forEach(storeCapabilities::add);
    for (Class<?> capability : storeCapabilities) {
      context.registerBean(
          "fullGraph." + capability.getName(), (Class) capability, () -> mock(capability));
    }
  }

  private static Object field(Object target, Class<?> declaringType, String name) {
    try {
      Field field = declaringType.getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException exception) {
      throw new AssertionError(
          "Unable to inspect " + declaringType.getName() + "." + name, exception);
    }
  }

  private static DeadLetterService injectedDeadLetterService(
      PostExecutionHandler postExecutionHandler) {
    try {
      PostExecutionHandler target = AopTestUtils.getTargetObject(postExecutionHandler);
      Field field = PostExecutionHandler.class.getDeclaredField("deadLetterService");
      field.setAccessible(true);
      return (DeadLetterService) field.get(target);
    } catch (ReflectiveOperationException exception) {
      throw new AssertionError(
          "Unable to inspect the managed PostExecutionHandler target", exception);
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  static class ManagedApplication {

    @Bean
    RecordingTransactionManager transactionManager() {
      return new RecordingTransactionManager();
    }

    @Bean
    JobBatchStatusStore jobBatchStatusStore() {
      return mock(JobBatchStatusStore.class);
    }

    @Bean
    NodeIdentityProvider nodeIdentityProvider() {
      NodeIdentityProvider provider = mock(NodeIdentityProvider.class);
      when(provider.getNodeId()).thenReturn("spring-node");
      return provider;
    }

    @Bean
    ExecutorProvider executorProvider() {
      return mock(ExecutorProvider.class);
    }

    @Bean
    JobBulkStore jobBulkStore() {
      return mock(JobBulkStore.class);
    }

    @Bean
    JobTerminalStore jobTerminalStore() {
      return mock(JobTerminalStore.class);
    }

    @Bean
    @Primary
    SingletonLeaseService singletonLeaseService() {
      return mock(SingletonLeaseService.class);
    }

    @Bean
    @Primary
    InternalEventPublisher internalEventPublisher() {
      return mock(InternalEventPublisher.class);
    }

    @Bean
    ErrorSanitizer errorSanitizer() {
      ErrorSanitizer sanitizer = mock(ErrorSanitizer.class);
      when(sanitizer.sanitize(any())).thenReturn("sanitized failure");
      return sanitizer;
    }

    @Bean
    Clock clock() {
      Clock clock = mock(Clock.class);
      when(clock.instant()).thenReturn(Instant.parse("2026-07-29T12:00:00Z"));
      return clock;
    }

    @Bean
    @Primary
    BatchService batchService() {
      return nonAdvisedMock(BatchService.class);
    }

    @Bean
    @Primary
    WorkflowScheduler workflowScheduler() {
      return nonAdvisedMock(WorkflowScheduler.class);
    }

    @Bean
    @Primary
    PollerScheduler pollerScheduler() {
      return mock(PollerScheduler.class);
    }

    // Transactional collaborator mocks are fixtures, not advice subjects. Without this marker,
    // proxy-target-class=false can wrap Mockito's MockAccess interface in a non-assignable JDK
    // proxy.
    private static <T> T nonAdvisedMock(Class<T> type) {
      return mock(type, withSettings().extraInterfaces(AopInfrastructureBean.class));
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class SelfInvocationOverrides {

    @Bean
    @Primary
    DeadLetterService nonTransactionalDeadLetterService() {
      return ManagedApplication.nonAdvisedMock(DeadLetterService.class);
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  static class FullGraphApplication {

    @Bean
    RecordingTransactionManager transactionManager() {
      return new RecordingTransactionManager();
    }
  }

  static final class RecordingTransactionManager extends AbstractPlatformTransactionManager {

    private final ThreadLocal<Object> currentTransaction = new ThreadLocal<>();
    private final List<String> events = new ArrayList<>();

    @Override
    protected Object doGetTransaction() throws TransactionException {
      return new TransactionObject(currentTransaction.get());
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
      return ((TransactionObject) transaction).resource != null;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition)
        throws TransactionException {
      Object resource = new Object();
      ((TransactionObject) transaction).resource = resource;
      currentTransaction.set(resource);
      events.add("BEGIN(" + propagationName(definition) + ")");
    }

    @Override
    protected Object doSuspend(Object transaction) throws TransactionException {
      TransactionObject transactionObject = (TransactionObject) transaction;
      Object suspended = transactionObject.resource;
      transactionObject.resource = null;
      currentTransaction.remove();
      events.add("SUSPEND");
      return suspended;
    }

    @Override
    protected void doResume(Object transaction, Object suspendedResources)
        throws TransactionException {
      ((TransactionObject) transaction).resource = suspendedResources;
      currentTransaction.set(suspendedResources);
      events.add("RESUME");
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
      events.add("COMMIT");
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
      events.add("ROLLBACK");
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
      TransactionObject transactionObject = (TransactionObject) transaction;
      if (currentTransaction.get() == transactionObject.resource) {
        currentTransaction.remove();
      }
      transactionObject.resource = null;
    }

    List<String> events() {
      return List.copyOf(events);
    }

    void clear() {
      events.clear();
    }

    private static String propagationName(TransactionDefinition definition) {
      return switch (definition.getPropagationBehavior()) {
        case TransactionDefinition.PROPAGATION_REQUIRED -> "REQUIRED";
        case TransactionDefinition.PROPAGATION_REQUIRES_NEW -> "REQUIRES_NEW";
        default ->
            throw new IllegalArgumentException(
                "Unexpected propagation " + definition.getPropagationBehavior());
      };
    }

    private static final class TransactionObject {

      private Object resource;

      private TransactionObject(Object resource) {
        this.resource = resource;
      }
    }
  }
}
