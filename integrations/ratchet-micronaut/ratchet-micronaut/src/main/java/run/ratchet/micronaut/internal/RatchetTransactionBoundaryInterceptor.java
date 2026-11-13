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
package run.ratchet.micronaut.internal;

import io.micronaut.aop.InterceptPhase;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.core.order.Ordered;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.interceptor.TransactionalInterceptor;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/** Runs every transaction type on Ratchet's manager, bypassing Micronaut Data's interceptor. */
@Singleton
@InterceptorBean(RatchetTransactionBoundary.class)
public final class RatchetTransactionBoundaryInterceptor
    implements MethodInterceptor<Object, Object>, Ordered {
  private final RatchetTransactionOperations transactions;
  private final TransactionalInterceptor transactionalInterceptor;
  private final ConcurrentHashMap<ExecutableMethod<?, ?>, Method> methods =
      new ConcurrentHashMap<>();

  public RatchetTransactionBoundaryInterceptor(
      RatchetTransactionOperations transactions,
      TransactionalInterceptor transactionalInterceptor) {
    this.transactions = transactions;
    // This internal singleton is required to skip Data's default-manager advice; injection fails
    // fast.
    this.transactionalInterceptor = transactionalInterceptor;
  }

  @Override
  public int getOrder() {
    return InterceptPhase.TRANSACTION.getPosition() - 1;
  }

  @Override
  public Object intercept(MethodInvocationContext<Object, Object> context) {
    Method method =
        methods.computeIfAbsent(
            context.getExecutableMethod(),
            executable -> {
              try {
                return context
                    .getTarget()
                    .getClass()
                    .getMethod(executable.getMethodName(), executable.getArgumentTypes());
              } catch (NoSuchMethodException failure) {
                throw new IllegalStateException(
                    "Cannot resolve transaction boundary method " + executable, failure);
              }
            });
    Transactional annotation = method.getAnnotation(Transactional.class);
    if (annotation == null)
      annotation = context.getTarget().getClass().getAnnotation(Transactional.class);
    if (annotation == null) return context.proceed();
    var returnType = context.getReturnType();
    if (context.isSuspend()
        || returnType.isAsync()
        || Future.class.isAssignableFrom(returnType.getType())
        || returnType.isReactive()) {
      throw new IllegalStateException(
          "Transaction boundary requires a synchronous method: " + method);
    }
    TransactionOperations<?> operations = transactions.operations();
    ExecutableMethod<?, ?> executable = context.getExecutableMethod();
    String name = executable.getDeclaringType().getSimpleName() + "." + executable.getMethodName();
    return operations.execute(
        MicronautTransactions.definition(operations, annotation, name),
        status -> skipMicronaut(context));
  }

  private Object skipMicronaut(MethodInvocationContext<Object, Object> context) {
    // @Type bindings also apply through stereotypes, including mapped Jakarta annotations.
    if (context.hasStereotype(io.micronaut.transaction.annotation.Transactional.class)) {
      return context.proceed(transactionalInterceptor);
    }
    return context.proceed();
  }
}
