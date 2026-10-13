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
package run.ratchet.micronaut.jpa;

import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.store.spi.JobStore;

/** JDK store proxy preserving implementation transactions and advised capability/default views. */
final class RatchetTransactionalStoreProxy {
  private RatchetTransactionalStoreProxy() {}

  static JobStore create(
      JobStore target,
      Class<? extends JobStore> compositeType,
      TransactionOperations<?> transactions) {
    return (JobStore) createProxy(target, compositeType, transactions);
  }

  static Object createProxy(
      Object target, Class<?> compositeType, TransactionOperations<?> transactions) {
    AtomicReference<Object> proxyReference = new AtomicReference<>();
    var defaultMethods = new ConcurrentHashMap<Method, MethodHandle>();
    var resolvedMethods = new ConcurrentHashMap<Method, Resolved>();
    Object proxy =
        Proxy.newProxyInstance(
            compositeType.getClassLoader(),
            new Class<?>[] {compositeType},
            (advised, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "equals" -> advised == args[0];
                  case "hashCode" -> System.identityHashCode(advised);
                  default -> "Ratchet transactional store[" + target + "]";
                };
              }
              if (method.getName().equals("capability")
                  && args != null
                  && args.length == 1
                  && args[0] instanceof Class<?> capability) {
                return capability.isInstance(advised) ? Optional.of(advised) : Optional.empty();
              }
              Resolved resolved = resolvedMethods.computeIfAbsent(method, m -> resolve(target, m));
              Method implementation = resolved.implementation();
              Transactional annotation = resolved.annotation();
              Invocation invocation =
                  () -> {
                    if (method.isDefault() && implementation.isDefault()) {
                      return defaultMethods
                          .computeIfAbsent(method, RatchetTransactionalStoreProxy::defaultMethod)
                          .bindTo(proxyReference.get())
                          .invokeWithArguments(args == null ? new Object[0] : args);
                    }
                    try {
                      return implementation.invoke(target, args);
                    } catch (InvocationTargetException failure) {
                      throw failure.getCause();
                    }
                  };
              if (annotation == null) return invocation.call();
              return invokeTransactional(
                  transactions,
                  MicronautTransactions.definition(transactions, annotation),
                  invocation);
            });
    proxyReference.set(proxy);
    return proxy;
  }

  private static Object invokeTransactional(
      TransactionOperations<?> transactions,
      TransactionDefinition definition,
      Invocation invocation)
      throws Throwable {
    AtomicReference<Throwable> original = new AtomicReference<>();
    try {
      // Both managers consult TransactionDefinition.rollbackOn for checked exceptions, including
      // joined REQUIRED transactions.
      return transactions.execute(
          definition,
          status -> {
            try {
              return invocation.call();
            } catch (Throwable failure) {
              original.set(failure);
              return sneakyThrow(failure);
            }
          });
    } catch (Throwable boundaryFailure) {
      if (original.get() instanceof RatchetTransientStoreException transientFailure
          && original.get() != boundaryFailure) {
        var failure =
            new RatchetTransientStoreException(
                "Transaction cleanup failed after a transient store failure", boundaryFailure);
        failure.addSuppressed(transientFailure);
        throw failure;
      }
      throw boundaryFailure;
    }
  }

  static Propagation propagation(TxType type) {
    return MicronautTransactions.propagation(type);
  }

  static TransactionDefinition definition(Transactional annotation) {
    return MicronautTransactions.definition(annotation);
  }

  /** Implementation method and its effective {@code @Transactional}, resolved once per method. */
  private record Resolved(Method implementation, Transactional annotation) {}

  private static Resolved resolve(Object target, Method method) {
    Method implementation;
    try {
      implementation = target.getClass().getMethod(method.getName(), method.getParameterTypes());
    } catch (NoSuchMethodException failure) {
      throw new IllegalStateException("Store implementation lacks " + method, failure);
    }
    // The SQL implementation classes are package-private, although their methods are public.
    if (!implementation.trySetAccessible()) {
      throw new IllegalStateException("Cannot invoke store implementation " + implementation);
    }
    Transactional annotation = implementation.getAnnotation(Transactional.class);
    if (annotation == null) annotation = target.getClass().getAnnotation(Transactional.class);
    return new Resolved(implementation, annotation);
  }

  private static MethodHandle defaultMethod(Method method) {
    try {
      Class<?> declaringClass = method.getDeclaringClass();
      return MethodHandles.privateLookupIn(declaringClass, MethodHandles.lookup())
          .unreflectSpecial(method, declaringClass);
    } catch (IllegalAccessException failure) {
      throw new IllegalStateException("Cannot invoke store default method " + method, failure);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Throwable> T sneakyThrow(Throwable failure) throws E {
    throw (E) failure;
  }

  @FunctionalInterface
  private interface Invocation {
    Object call() throws Throwable;
  }
}
