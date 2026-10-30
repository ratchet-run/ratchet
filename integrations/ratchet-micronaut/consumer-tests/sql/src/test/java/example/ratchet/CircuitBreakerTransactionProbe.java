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
package example.ratchet;

import io.micronaut.aop.InterceptPhase;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.order.Ordered;
import io.micronaut.transaction.annotation.Transactional;
import jakarta.inject.Singleton;

/** Counts entry into transaction advice after circuit protection, before a transaction starts. */
@Singleton
@Requires(property = "consumer.circuit-breaker", value = "true")
@InterceptorBean(Transactional.class)
public final class CircuitBreakerTransactionProbe
    implements MethodInterceptor<Object, Object>, Ordered {
  private final ThreadLocal<Integer> entries = ThreadLocal.withInitial(() -> 0);

  /** Returns advice entries on the caller thread, excluding background scheduler transactions. */
  public int entries() {
    return entries.get();
  }

  @Override
  public int getOrder() {
    return InterceptPhase.TRANSACTION.getPosition() - 1;
  }

  @Override
  public Object intercept(MethodInvocationContext<Object, Object> context) {
    if (context.getTarget() instanceof CircuitBreakerBeanTest.ProtectedService
        && context.getMethodName().equals("transactionalCall")) entries.set(entries.get() + 1);
    return context.proceed();
  }
}
