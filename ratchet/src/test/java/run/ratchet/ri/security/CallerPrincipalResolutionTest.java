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
package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class CallerPrincipalResolutionTest {

  @Test
  void resolve_resolverPresent_takesPrecedenceOverProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));

    Optional<String> result =
        CallerPrincipalResolution.resolve(() -> Optional.of("resolver-principal"), provider);

    assertEquals(Optional.of("resolver-principal"), result);
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_resolverThrows_degradesToEmptyWithoutConsultingProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);

    Optional<String> result =
        CallerPrincipalResolution.resolve(
            () -> {
              throw new IllegalStateException("ContextNotActiveException-like failure");
            },
            provider);

    assertTrue(result.isEmpty(), "A throwing resolver must degrade to empty, not propagate");
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_resolverReturnsNullOptional_isTreatedAsEmpty() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);

    Optional<String> result = CallerPrincipalResolution.resolve(() -> null, provider);

    assertTrue(result.isEmpty(), "A resolver returning a null Optional must be treated as empty");
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_resolverNull_fallsBackToProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));

    Optional<String> result = CallerPrincipalResolution.resolve(null, provider);

    assertEquals(Optional.of("provider-principal"), result);
  }

  @Test
  void resolve_resolverAndProviderNull_yieldsEmpty() {
    Optional<String> result = CallerPrincipalResolution.resolve(null, null);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolve_resolverNullAndProviderEmpty_yieldsEmpty() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.empty());

    Optional<String> result = CallerPrincipalResolution.resolve(null, provider);

    assertTrue(result.isEmpty());
  }
}
