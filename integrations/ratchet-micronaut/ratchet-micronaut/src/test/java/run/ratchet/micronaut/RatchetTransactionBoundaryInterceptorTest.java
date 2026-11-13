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
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.ReturnType;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.transaction.TransactionCallback;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionOperationsRegistry;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.exceptions.IllegalTransactionStateException;
import io.micronaut.transaction.interceptor.TransactionalInterceptor;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import run.ratchet.micronaut.internal.RatchetTransactionBoundaryInterceptor;
import run.ratchet.micronaut.internal.RatchetTransactionOperations;

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
    verify(operations)
        .execute(argThat(d -> d.getPropagationBehavior() == Propagation.SUPPORTS), any());
  }

  @Test
  void mandatoryRejectsMissingTransactionAndRequiredJoinsRealTransaction() {
    TransactionOperations<Object> operations = operations();
    var interceptor = interceptor(operations);
    var mandatory = invocation(new Child(), "mandatory");
    assertThrows(IllegalTransactionStateException.class, () -> interceptor.intercept(mandatory));
    verify(mandatory, never()).proceed();
    verify(operations, never()).execute(any(), any());
    TransactionStatus<?> status = mock(TransactionStatus.class);
    doReturn(Optional.of(status)).when(operations).findTransactionStatus();
    var required = invocation(new Child(), "required");
    when(required.proceed()).thenReturn("joined");
    assertEquals("joined", interceptor.intercept(required));
    verify(operations)
        .execute(argThat(d -> d.getPropagationBehavior() == Propagation.REQUIRED), any());
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
    RatchetTransactionOperations transactions = mock(RatchetTransactionOperations.class);
    doReturn(operations).when(transactions).operations();
    return new RatchetTransactionBoundaryInterceptor(transactions, dataInterceptor());
  }

  @SuppressWarnings("unchecked")
  private static MethodInvocationContext<Object, Object> invocation(Object target, String name) {
    MethodInvocationContext<Object, Object> context = mock(MethodInvocationContext.class);
    ExecutableMethod<Object, Object> method = mock(ExecutableMethod.class);
    when(method.getMethodName()).thenReturn(name);
    doReturn(Child.class).when(method).getDeclaringType();
    doReturn(ReturnType.of(Object.class)).when(context).getReturnType();
    when(method.getArgumentTypes()).thenReturn(new Class<?>[0]);
    when(context.getExecutableMethod()).thenReturn(method);
    when(context.getTarget()).thenReturn(target);
    return context;
  }

  private static TransactionalInterceptor dataInterceptor() {
    return new TransactionalInterceptor(
        mock(TransactionOperationsRegistry.class), null, ConversionService.SHARED);
  }

  @Test
  void everyPropagationExecutesOnSelectedManager() {
    for (String name :
        new String[] {
          "required", "mandatory", "requiresNew", "notSupported", "supports", "never"
        }) {
      TransactionOperations<Object> operations = operations();
      doReturn(Optional.of(mock(TransactionStatus.class))).when(operations).findTransactionStatus();
      var context = invocation(new Child(), name);
      interceptor(operations).intercept(context);
      Transactional annotation;
      try {
        annotation = Parent.class.getMethod(name).getAnnotation(Transactional.class);
      } catch (NoSuchMethodException failure) {
        throw new AssertionError(failure);
      }
      TxType type = annotation == null ? TxType.REQUIRED : annotation.value();
      verify(operations)
          .execute(
              argThat(
                  d ->
                      d.getPropagationBehavior().name().equals(type.name())
                          && d.getName().equals("Child." + name)),
              any());
      verify(context).proceed();
    }
  }

  @Test
  void skipsDataInterceptorOnlyWhenTransactionStereotypeIsBound() {
    TransactionOperations<Object> operations = operations();
    RatchetTransactionOperations transactions = mock(RatchetTransactionOperations.class);
    doReturn(operations).when(transactions).operations();
    TransactionalInterceptor data = dataInterceptor();
    var interceptor = new RatchetTransactionBoundaryInterceptor(transactions, data);
    var context = invocation(new Child(), "required");
    when(context.hasStereotype(io.micronaut.transaction.annotation.Transactional.class))
        .thenReturn(true);
    when(context.proceed(data)).thenReturn("skipped");
    assertEquals("skipped", interceptor.intercept(context));
    verify(context).proceed(data);
    verify(context, never()).proceed();
  }

  @Test
  void unannotatedMethodSimplyProceeds() {
    TransactionOperations<Object> operations = operations();
    var context = invocation(new Unannotated(), "plain");
    when(context.proceed()).thenReturn("plain");
    assertEquals("plain", interceptor(operations).intercept(context));
    verify(operations, never()).execute(any(), any());
  }

  @Test
  void rejectsAsyncAndReactiveMethodsBeforeExecuting() {
    for (Class<?> type : new Class<?>[] {CompletionStage.class, Publisher.class}) {
      TransactionOperations<Object> operations = operations();
      var context = invocation(new Child(), "required");
      doReturn(ReturnType.of(type)).when(context).getReturnType();
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class, () -> interceptor(operations).intercept(context));
      assertTrue(failure.getMessage().contains("required"));
      verify(operations, never()).execute(any(), any());
      verify(context, never()).proceed();
    }
  }

  public static class Unannotated {
    public void plain() {}
  }

  @Transactional
  public static class Parent {
    public void required() throws IOException {}

    @Transactional(TxType.SUPPORTS)
    public void supports() {}

    @Transactional(TxType.REQUIRES_NEW)
    public void requiresNew() {}

    @Transactional(TxType.NOT_SUPPORTED)
    public void notSupported() {}

    @Transactional(TxType.NEVER)
    public void never() {}

    @Transactional(TxType.MANDATORY)
    public void mandatory() {}
  }

  public static class Child extends Parent {}
}
