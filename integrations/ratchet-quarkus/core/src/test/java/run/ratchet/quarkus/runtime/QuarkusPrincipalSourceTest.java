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
package run.ratchet.quarkus.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Proxy;
import java.security.Principal;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class QuarkusPrincipalSourceTest {

  @Test
  void currentPrincipalReturnsAuthenticatedRequestIdentity() {
    Principal principal = () -> "alice";
    SecurityIdentity identity =
        (SecurityIdentity)
            Proxy.newProxyInstance(
                QuarkusPrincipalSourceTest.class.getClassLoader(),
                new Class<?>[] {SecurityIdentity.class},
                (proxy, method, arguments) ->
                    switch (method.getName()) {
                      case "isAnonymous" -> false;
                      case "getPrincipal" -> principal;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });

    var source = new QuarkusPrincipalSource();
    source.identities = resolvedInstance(identity);

    assertEquals(Optional.of("alice"), source.currentPrincipal());
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> resolvedInstance(T value) {
    return (Instance<T>)
        Proxy.newProxyInstance(
            QuarkusPrincipalSourceTest.class.getClassLoader(),
            new Class<?>[] {Instance.class},
            (proxy, method, arguments) -> {
              switch (method.getName()) {
                case "isResolvable":
                  return true;
                case "get":
                  return value;
                default:
                  throw new UnsupportedOperationException(method.getName());
              }
            });
  }
}
