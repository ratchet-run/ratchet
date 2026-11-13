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

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.context.ServerRequestContext;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.filters.SecurityFilter;
import io.micronaut.security.utils.DefaultSecurityService;
import io.micronaut.security.utils.SecurityService;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.PrincipalSource;

class RatchetSecurityConfigurationTest {
  @Test
  void bridgeResolvesAuthenticatedRequestAndNoOtherCaller() {
    try (var context = TestContexts.engine(Map.of())) {
      context.start();
      assertInstanceOf(DefaultSecurityService.class, context.getBean(SecurityService.class));
      assertEquals(1, context.getBeansOfType(PrincipalSource.class).size());
      assertInstanceOf(
          MicronautSecurityPrincipalSource.class, context.getBean(PrincipalSource.class));
      var provider = context.getBean(CallerPrincipalProvider.class);
      assertEquals(Optional.empty(), provider.currentPrincipal());
      assertEquals(Optional.of("alice"), authenticated("alice", provider::currentPrincipal));
      Supplier<Optional<String>> current = provider::currentPrincipal;
      assertEquals(Optional.empty(), ServerRequestContext.with(HttpRequest.GET("/"), current));
      assertEquals(Optional.empty(), provider.currentPrincipal());
    }
  }

  @Test
  void applicationSourceWinsInsideAuthenticatedRequest() {
    PrincipalSource application = () -> Optional.of("application-user");
    try (var context = TestContexts.engine(Map.of())) {
      context.registerSingleton(PrincipalSource.class, application);
      context.start();
      assertSame(application, context.getBean(PrincipalSource.class));
      var provider = context.getBean(CallerPrincipalProvider.class);
      assertEquals(
          Optional.of("application-user"), authenticated("alice", provider::currentPrincipal));
    }
  }

  @Test
  void disabledSecurityKeepsBridgeAbsent() {
    try (var context = TestContexts.core(Map.of("micronaut.security.enabled", false))) {
      context.start();
      assertFalse(context.containsBean(SecurityService.class));
      assertFalse(context.containsBean(PrincipalSource.class));
    }
  }

  @Test
  void disabledRatchetDisablesBridge() {
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false))) {
      context.start();
      assertFalse(context.containsBean(PrincipalSource.class));
    }
  }

  @Test
  void missingSecurityClassesKeepBridgeAbsent() {
    ClassLoader loader =
        new ClassLoader(getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("io.micronaut.security.")) throw new ClassNotFoundException(name);
            return super.loadClass(name, resolve);
          }
        };
    try (var context =
        ApplicationContext.builder()
            .classLoader(loader)
            .deduceEnvironment(false)
            .enableDefaultPropertySources(false)
            .properties(Map.of("ratchet.allowed-packages", "example.jobs"))
            .build()) {
      context.start();
      assertFalse(context.containsBean(PrincipalSource.class));
    }
  }

  private static <T> T authenticated(String name, Supplier<T> action) {
    // Micronaut Security 4 reads the request principal; 5 reads the SecurityFilter attribute.
    MutableHttpRequest<?> request = HttpRequest.GET("/");
    Authentication authentication = Authentication.build(name);
    request.setUserPrincipal(authentication);
    request.setAttribute(SecurityFilter.AUTHENTICATION, authentication);
    return ServerRequestContext.with(request, action);
  }
}
