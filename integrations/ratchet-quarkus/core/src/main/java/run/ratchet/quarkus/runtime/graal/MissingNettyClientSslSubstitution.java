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
package run.ratchet.quarkus.runtime.graal;

import java.util.Arrays;
import java.util.function.BooleanSupplier;

/** Enables the backport only when Netty has the overload and Quarkus has not supplied it. */
public final class MissingNettyClientSslSubstitution implements BooleanSupplier {
  @Override
  public boolean getAsBoolean() {
    try {
      ClassLoader loader = getClass().getClassLoader();
      return hasStartTlsOverload(Class.forName("io.netty.handler.ssl.SslContext", false, loader))
          && !hasStartTlsOverload(
              Class.forName(
                  "io.quarkus.netty.runtime.graal.Target_io_netty_handler_ssl_SslContext",
                  false,
                  loader));
    } catch (ClassNotFoundException | LinkageError unavailable) {
      return false;
    }
  }

  static boolean hasStartTlsOverload(Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .anyMatch(
            method -> {
              Class<?>[] parameters = method.getParameterTypes();
              return method.getName().equals("newClientContextInternal")
                  && parameters.length == 20
                  && parameters[14] == boolean.class
                  && parameters[15] == boolean.class;
            });
  }
}
