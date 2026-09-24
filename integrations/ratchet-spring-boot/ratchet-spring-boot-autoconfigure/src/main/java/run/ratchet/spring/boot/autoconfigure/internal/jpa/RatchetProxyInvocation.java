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
package run.ratchet.spring.boot.autoconfigure.internal.jpa;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Shared identity and exception semantics for persistence metadata proxies. */
final class RatchetProxyInvocation {
  private RatchetProxyInvocation() {}

  static Object forward(Object proxy, Object target, Method method, Object[] args)
      throws Throwable {
    if (method.getDeclaringClass() == Object.class) {
      if (method.getName().equals("equals")) return proxy == args[0];
      if (method.getName().equals("hashCode")) return System.identityHashCode(proxy);
    }
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException failure) {
      throw failure.getCause();
    }
  }
}
