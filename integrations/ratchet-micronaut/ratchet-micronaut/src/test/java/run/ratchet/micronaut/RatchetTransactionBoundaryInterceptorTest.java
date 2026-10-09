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
import static org.mockito.Mockito.*;

import io.micronaut.aop.InterceptPhase;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.transaction.TransactionCallback;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionOperationsRegistry;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.exceptions.IllegalTransactionStateException;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.micronaut.internal.RatchetTransactionBoundaryInterceptor;

class RatchetTransactionBoundaryInterceptorTest {
  @Test
  void requiredUsesInheritedClassDefaultBeforeMicronautAndKeepsCheckedException() {
    TransactionOperations<Object> operations = operations();
    var interceptor = interceptor(operations);
    assertTrue(interceptor.getOrder() < InterceptPhase.TRANSACTION.getPosition());
    var context = invocation(new Child(), "required");
    IOException failure = new IOException("original");
    doAnswer(
            ignored -> {
              throw failure;
            })
        .when(context)
        .proceed();
    assertSame(failure, assertThrows(IOException.class, () -> interceptor.intercept(context)));
    verify(operations)
        .execute(
            argThat(definition -> definition.getPropagationBehavior() == Propagation.REQUIRES_NEW),
            any());
  }

  @Test
  void targetMethodAnnotationOverridesCompiledAndClassMetadata() {
    TransactionOperations<Object> operations = operations();
    var interceptor = interceptor(operations);
    var context = invocation(new Child(), "supports");
    when(context.proceed()).thenReturn("result");
    assertEquals("result", interceptor.intercept(context));
    verify(operations, never()).execute(any(), any());
  }

  @Test
  void mandatoryRejectsMissingTransactionAndRequiredJoinsRealTransaction() {
    TransactionOperations<Object> operations = operations();
    var interceptor = interceptor(operations);
    var mandatory = invocation(new Child(), "mandatory");
    assertThrows(IllegalTransactionStateException.class, () -> interceptor.intercept(mandatory));
    verify(mandatory, never()).proceed();
    TransactionStatus<?> status = mock(TransactionStatus.class);
    doReturn(Optional.of(status)).when(operations).findTransactionStatus();
    var required = invocation(new Child(), "required");
    when(required.proceed()).thenReturn("joined");
    assertEquals("joined", interceptor.intercept(required));
    verify(operations, never()).execute(any(), any());
  }

  @SuppressWarnings("unchecked")
  private static TransactionOperations<Object> operations() {
    TransactionOperations<Object> operations = mock(TransactionOperations.class);
    when(operations.findTransactionStatus()).thenReturn(Optional.empty());
    doAnswer(
            invocation -> {
              TransactionCallback<Object, Object> callback = invocation.getArgument(1);
              return callback.apply(null);
            })
        .when(operations)
        .execute(any(TransactionDefinition.class), any());
    return operations;
  }

  private static RatchetTransactionBoundaryInterceptor interceptor(
      TransactionOperations<Object> operations) {
    TransactionOperationsRegistry registry = mock(TransactionOperationsRegistry.class);
    doReturn(operations).when(registry).provideSynchronous(TransactionOperations.class, null);
    return new RatchetTransactionBoundaryInterceptor(registry);
  }

  @SuppressWarnings("unchecked")
  private static MethodInvocationContext<Object, Object> invocation(Object target, String name) {
    MethodInvocationContext<Object, Object> context = mock(MethodInvocationContext.class);
    ExecutableMethod<Object, Object> method = mock(ExecutableMethod.class);
    when(method.getMethodName()).thenReturn(name);
    when(method.getArgumentTypes()).thenReturn(new Class<?>[0]);
    when(context.getExecutableMethod()).thenReturn(method);
    when(context.getTarget()).thenReturn(target);
    return context;
  }

  @Transactional
  public static class Parent {
    public void required() throws IOException {}

    @Transactional(TxType.SUPPORTS)
    public void supports() {}

    @Transactional(TxType.MANDATORY)
    public void mandatory() {}
  }

  public static class Child extends Parent {}
}
