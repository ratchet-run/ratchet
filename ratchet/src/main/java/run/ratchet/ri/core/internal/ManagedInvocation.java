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
package run.ratchet.ri.core.internal;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import run.ratchet.api.DoNotRetry;

/** Selects methods exposed by the managed proxy without bypassing container interception. */
public final class ManagedInvocation {
  private static final ClassValue<ConcurrentMap<Method, Optional<Method>>> EXPOSED_METHODS =
      new ClassValue<>() {
        @Override
        protected ConcurrentMap<Method, Optional<Method>> computeValue(Class<?> type) {
          return new ConcurrentHashMap<>();
        }
      };

  private ManagedInvocation() {}

  public static Method exposedMethod(Method method, Object instance) throws NoSuchMethodException {
    if (method.getDeclaringClass().isInstance(instance)) return method;
    Optional<Method> exposed =
        EXPOSED_METHODS
            .get(instance.getClass())
            .computeIfAbsent(method, declared -> findExposedMethod(declared, instance.getClass()));
    if (exposed.isPresent()) return exposed.get();
    throw new UnexposedMethodException(
        "Managed proxy does not expose "
            + method
            + "; expose this method on a proxy interface or use a class-based proxy");
  }

  /** Indicates that the managed proxy cannot expose the requested job method. */
  @DoNotRetry
  public static final class UnexposedMethodException extends NoSuchMethodException {
    public UnexposedMethodException(String message) {
      super(message);
    }
  }

  private static Optional<Method> findExposedMethod(Method method, Class<?> proxyClass) {
    for (Class<?> type : proxyClass.getInterfaces()) {
      boolean exposes =
          Arrays.stream(type.getMethods())
              .anyMatch(
                  candidate ->
                      candidate.getName().equals(method.getName())
                          && Arrays.equals(
                              candidate.getParameterTypes(), method.getParameterTypes()));
      if (exposes) {
        try {
          // Preserve getMethod's choice when covariant return types expose multiple matches.
          return Optional.of(type.getMethod(method.getName(), method.getParameterTypes()));
        } catch (NoSuchMethodException impossible) {
          throw new IllegalStateException("Exposed interface method disappeared", impossible);
        }
      }
    }
    return Optional.empty();
  }
}
