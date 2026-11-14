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
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

class SpringSecurityPrincipalSourceTest {
  private final SpringSecurityPrincipalSource source = new SpringSecurityPrincipalSource();

  @AfterEach
  void clearContext() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void authenticatedUserProvidesName() {
    SecurityContextHolder.getContext()
        .setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("alice", "", List.of()));
    assertThat(source.currentPrincipal()).contains("alice");
  }

  @Test
  void anonymousUserProvidesNoName() {
    SecurityContextHolder.getContext()
        .setAuthentication(
            new AnonymousAuthenticationToken(
                "key", "anonymousUser", List.of(new SimpleGrantedAuthority("ROLE_ANONYMOUS"))));
    assertThat(source.currentPrincipal()).isEmpty();
  }

  @Test
  void unauthenticatedUserProvidesNoName() {
    SecurityContextHolder.getContext()
        .setAuthentication(UsernamePasswordAuthenticationToken.unauthenticated("alice", ""));
    assertThat(source.currentPrincipal()).isEmpty();
  }

  @Test
  void blankNameProvidesNoName() {
    SecurityContextHolder.getContext()
        .setAuthentication(UsernamePasswordAuthenticationToken.authenticated("  ", "", List.of()));
    assertThat(source.currentPrincipal()).isEmpty();
  }

  @Test
  void emptyContextProvidesNoName() {
    SecurityContextHolder.clearContext();
    assertThat(source.currentPrincipal()).isEmpty();
  }

  @Test
  void nullNameProvidesNoName() {
    Authentication authentication = mock(Authentication.class);
    when(authentication.isAuthenticated()).thenReturn(true);
    SecurityContextHolder.getContext().setAuthentication(authentication);
    assertThat(source.currentPrincipal()).isEmpty();
  }

  @Test
  void lookupFailureProvidesNoName() {
    Authentication authentication = mock(Authentication.class);
    when(authentication.isAuthenticated()).thenThrow(new IllegalStateException("lookup failed"));
    SecurityContextHolder.getContext().setAuthentication(authentication);
    assertThat(source.currentPrincipal()).isEmpty();
  }
}
