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
package run.ratchet.spring.boot.autoconfigure.jpa;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.spi.JobStore;

/** Creates a JDK transaction proxy while keeping capability views on that same proxy. */
final class RatchetTransactionalStoreProxy {

  private RatchetTransactionalStoreProxy() {}

  static JobStore create(
      JobStore target,
      Class<? extends JobStore> compositeType,
      PlatformTransactionManager transactionManager) {
    return (JobStore) createProxy(target, compositeType, transactionManager);
  }

  static Object createProxy(
      Object target, Class<?> compositeType, PlatformTransactionManager transactionManager) {
    ProxyFactory proxyFactory = new ProxyFactory();
    proxyFactory.setTarget(target);
    proxyFactory.setInterfaces(compositeType);
    AtomicReference<Object> proxyReference = new AtomicReference<>();
    proxyFactory.addAdvice(
        (MethodInterceptor)
            invocation -> {
              if (invocation.getMethod().getName().equals("capability")
                  && invocation.getArguments().length == 1
                  && invocation.getArguments()[0] instanceof Class<?> capabilityType) {
                Object proxy = proxyReference.get();
                return proxy != null && capabilityType.isInstance(proxy)
                    ? Optional.of(capabilityType.cast(proxy))
                    : Optional.empty();
              }
              return invocation.proceed();
            });
    proxyFactory.addAdvice(
        new TransactionInterceptor(transactionManager, new AnnotationTransactionAttributeSource()) {
          @Override
          protected Object invokeWithinTransaction(
              Method method, Class<?> targetClass, InvocationCallback invocation) throws Throwable {
            var storeFailure = new AtomicReference<RatchetTransientStoreException>();
            try {
              return super.invokeWithinTransaction(
                  method,
                  targetClass,
                  () -> {
                    try {
                      return invocation.proceedWithInvocation();
                    } catch (RatchetTransientStoreException failure) {
                      storeFailure.set(failure);
                      throw failure;
                    }
                  });
            } catch (RuntimeException boundaryFailure) {
              if (storeFailure.get() == null || storeFailure.get() == boundaryFailure)
                throw boundaryFailure;
              // A broken connection can also fail rollback, replacing the translated store error
              // with a framework exception that has neither its cause nor its SQL state.
              var failure =
                  new RatchetTransientStoreException(
                      "Transaction cleanup failed after a transient store failure",
                      boundaryFailure);
              failure.addSuppressed(storeFailure.get());
              throw failure;
            }
          }
        });
    proxyFactory.addAdvice(
        (MethodInterceptor)
            invocation -> {
              if (invocation.getMethod().isDefault()
                  && target
                      .getClass()
                      .getMethod(
                          invocation.getMethod().getName(),
                          invocation.getMethod().getParameterTypes())
                      .isDefault()) {
                return InvocationHandler.invokeDefault(
                    proxyReference.get(), invocation.getMethod(), invocation.getArguments());
              }
              return invocation.proceed();
            });
    Object proxy = proxyFactory.getProxy(compositeType.getClassLoader());
    proxyReference.set(proxy);
    return proxy;
  }
}
