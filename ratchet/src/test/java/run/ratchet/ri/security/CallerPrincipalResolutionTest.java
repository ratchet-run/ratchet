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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobContext;

class CallerPrincipalResolutionTest {

  @AfterEach
  void clearJobContext() {
    JobContext.clear();
  }

  @Test
  void resolve_resolverPresent_takesPrecedenceOverJobContextAndProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));
    JobContext.bind(
        UUID.randomUUID(), null, Map.of(), "job-context-principal", /* signalPayload */ null);

    Optional<String> result =
        CallerPrincipalResolution.resolve(() -> Optional.of("resolver-principal"), provider);

    assertEquals(Optional.of("resolver-principal"), result);
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_resolverEmpty_fallsBackToJobContextBeforeProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));
    JobContext.bind(
        UUID.randomUUID(), null, Map.of(), "job-context-principal", /* signalPayload */ null);

    Optional<String> result = CallerPrincipalResolution.resolve(Optional::empty, provider);

    assertEquals(Optional.of("job-context-principal"), result);
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_resolverReturnsNullOptional_fallsBackToJobContext() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    JobContext.bind(
        UUID.randomUUID(), null, Map.of(), "job-context-principal", /* signalPayload */ null);

    Optional<String> result = CallerPrincipalResolution.resolve(() -> null, provider);

    assertEquals(
        Optional.of("job-context-principal"),
        result,
        "A resolver returning a null Optional must be treated as empty");
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_jobContextEmpty_fallsBackToProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));
    JobContext.bind(UUID.randomUUID(), null, Map.of(), null, /* signalPayload */ null);

    Optional<String> result = CallerPrincipalResolution.resolve(null, provider);

    assertEquals(Optional.of("provider-principal"), result);
  }

  @Test
  void resolve_noJobContext_fallsBackToProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));

    Optional<String> result = CallerPrincipalResolution.resolve(null, provider);

    assertEquals(Optional.of("provider-principal"), result);
  }

  @Test
  void resolve_allSourcesEmpty_yieldsEmpty() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.empty());
    JobContext.bind(UUID.randomUUID(), null, Map.of(), null, /* signalPayload */ null);

    Optional<String> result = CallerPrincipalResolution.resolve(Optional::empty, provider);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolve_resolverAndProviderNull_yieldsEmpty() {
    Optional<String> result = CallerPrincipalResolution.resolve(null, null);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolve_throwingResolver_degradesToJobContext() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    JobContext.bind(
        UUID.randomUUID(), null, Map.of(), "job-context-principal", /* signalPayload */ null);

    Optional<String> result =
        CallerPrincipalResolution.resolve(
            () -> {
              throw new IllegalStateException("ContextNotActiveException-like failure");
            },
            provider);

    assertEquals(
        Optional.of("job-context-principal"),
        result,
        "A throwing resolver must degrade to the next source, not propagate");
    verify(provider, never()).currentPrincipal();
  }

  @Test
  void resolve_throwingResolverWithNoJobContext_degradesToProvider() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenReturn(Optional.of("provider-principal"));

    Optional<String> result =
        CallerPrincipalResolution.resolve(
            () -> {
              throw new IllegalStateException("ContextNotActiveException-like failure");
            },
            provider);

    assertEquals(Optional.of("provider-principal"), result);
  }

  @Test
  void resolve_throwingProvider_degradesToEmpty() {
    CallerPrincipalProvider provider = mock(CallerPrincipalProvider.class);
    when(provider.currentPrincipal()).thenThrow(new IllegalStateException("provider failure"));

    Optional<String> result = CallerPrincipalResolution.resolve(null, provider);

    assertTrue(result.isEmpty(), "A throwing provider must degrade to empty, not propagate");
  }
}
