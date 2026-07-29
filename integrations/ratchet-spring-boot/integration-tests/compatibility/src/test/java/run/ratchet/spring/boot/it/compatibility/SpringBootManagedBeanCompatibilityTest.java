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
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.AopTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobTerminalStore;

class SpringBootManagedBeanCompatibilityTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(ManagedApplication.class);

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
  @EnableAutoConfiguration
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
    SingletonLeaseService singletonLeaseService() {
      return mock(SingletonLeaseService.class);
    }

    @Bean
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
    BatchService batchService() {
      return nonAdvisedMock(BatchService.class);
    }

    @Bean
    WorkflowScheduler workflowScheduler() {
      return nonAdvisedMock(WorkflowScheduler.class);
    }

    @Bean
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
