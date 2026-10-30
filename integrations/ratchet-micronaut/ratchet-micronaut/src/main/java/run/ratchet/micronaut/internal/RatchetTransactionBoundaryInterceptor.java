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
import io.micronaut.transaction.TransactionOperationsRegistry;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/** Establishes a real REQUIRED boundary before Micronaut Data joins the current scope. */
@Singleton
@InterceptorBean(RatchetTransactionBoundary.class)
public final class RatchetTransactionBoundaryInterceptor
    implements MethodInterceptor<Object, Object>, Ordered {
  private final TransactionOperationsRegistry registry;
  private final ConcurrentHashMap<ExecutableMethod<?, ?>, Method> methods =
      new ConcurrentHashMap<>();

  public RatchetTransactionBoundaryInterceptor(TransactionOperationsRegistry registry) {
    this.registry = registry;
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
    TxType type = annotation.value();
    TransactionOperations<?> operations =
        registry.provideSynchronous(TransactionOperations.class, null);
    if ((type == TxType.REQUIRED || type == TxType.MANDATORY)
        && !MicronautTransactions.hasActiveTransaction(operations)) {
      return operations.execute(
          MicronautTransactions.definition(operations, annotation), status -> context.proceed());
    }
    return context.proceed();
  }
}
