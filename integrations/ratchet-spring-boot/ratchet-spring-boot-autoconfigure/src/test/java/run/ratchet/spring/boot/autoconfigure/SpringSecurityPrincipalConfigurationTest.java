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
package run.ratchet.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.PrincipalSource;
import run.ratchet.store.spi.JobStore;

class SpringSecurityPrincipalConfigurationTest {
  @AfterEach
  void clearContext() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void springSecurityEnablesBridge() {
    SecurityContextHolder.getContext()
        .setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("alice", "", List.of()));
    runner()
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(PrincipalSource.class);
              assertThat(context.getBean(PrincipalSource.class))
                  .isInstanceOf(SpringSecurityPrincipalSource.class);
              assertThat(context.getBean(CallerPrincipalProvider.class).currentPrincipal())
                  .contains("alice");
            });
  }

  @Test
  void applicationSourceReplacesBridge() {
    PrincipalSource custom = () -> Optional.of("application");
    runner()
        .withBean(PrincipalSource.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(PrincipalSource.class);
              assertThat(context).doesNotHaveBean("springSecurityPrincipalSource");
              assertThat(context.getBean(PrincipalSource.class)).isSameAs(custom);
              assertThat(context.getBean(CallerPrincipalProvider.class).currentPrincipal())
                  .contains("application");
            });
  }

  @Test
  void missingSpringSecurityStartsWithoutBridge() {
    runner()
        .withClassLoader(new FilteredClassLoader("org.springframework.security"))
        .run(
            context -> {
              assertThat(context).hasNotFailed().doesNotHaveBean(PrincipalSource.class);
              assertThat(context.getBean(CallerPrincipalProvider.class).currentPrincipal())
                  .isEmpty();
            });
  }

  private ApplicationContextRunner runner() {
    return new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class, RatchetEngineAutoConfiguration.class))
        .withPropertyValues("ratchet.allowed-packages=example.jobs")
        .withBean(JobStore.class, () -> mock(JobStore.class))
        .withBean(RatchetRuntime.class, () -> mock(RatchetRuntime.class));
  }
}
