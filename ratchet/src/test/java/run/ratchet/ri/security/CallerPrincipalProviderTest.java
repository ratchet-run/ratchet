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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PrincipalSource;

class CallerPrincipalProviderTest {

  private static final class UnmanagedCallerPrincipalProvider extends CallerPrincipalProvider {}

  @Test
  void currentPrincipal_resolvableSource_returnsName() {
    PrincipalSource source = mock(PrincipalSource.class);
    when(source.currentPrincipal()).thenReturn(Optional.of("alice"));

    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    handle(instance, source, ApplicationScoped.class);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertEquals(Optional.of("alice"), result);
  }

  @Test
  void currentPrincipal_sourceReturnsEmpty_returnsEmpty() {
    PrincipalSource source = mock(PrincipalSource.class);
    when(source.currentPrincipal()).thenReturn(Optional.empty());

    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    handle(instance, source, ApplicationScoped.class);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Empty source should yield empty");
  }

  @Test
  void currentPrincipal_notResolvable_returnsEmpty() {
    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(false);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Unresolvable PrincipalSource should yield empty");
  }

  @Test
  void currentPrincipal_sourceLookupFailure_returnsEmpty() {
    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.getHandle()).thenThrow(new IllegalStateException("container is shutting down"));

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "PrincipalSource failures should not block job creation");
  }

  @Test
  void currentPrincipal_sourceFailure_returnsEmpty() {
    PrincipalSource source = mock(PrincipalSource.class);
    when(source.currentPrincipal()).thenThrow(new RuntimeException("source failed"));

    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    handle(instance, source, ApplicationScoped.class);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "PrincipalSource failures should not block job creation");
  }

  @Test
  void currentPrincipal_unmanagedNoArgConstruction_throws() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> new UnmanagedCallerPrincipalProvider().currentPrincipal());

    assertEquals("PrincipalSource Instance was not injected", exception.getMessage());
  }

  @Test
  void currentPrincipal_emptyPrincipalName_returnsEmpty() {
    PrincipalSource source = mock(PrincipalSource.class);
    when(source.currentPrincipal()).thenReturn(Optional.of(""));

    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    handle(instance, source, ApplicationScoped.class);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Empty principal name should yield empty");
  }

  @Test
  void currentPrincipal_dependentSource_destroysHandle() {
    PrincipalSource source = mock(PrincipalSource.class);
    when(source.currentPrincipal()).thenReturn(Optional.of("alice"));

    @SuppressWarnings("unchecked")
    Instance<PrincipalSource> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    Instance.Handle<PrincipalSource> handle = handle(instance, source, Dependent.class);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertEquals(Optional.of("alice"), result);
    verify(handle).destroy();
  }

  @SuppressWarnings("unchecked")
  private static Instance.Handle<PrincipalSource> handle(
      Instance<PrincipalSource> instance,
      PrincipalSource source,
      Class<? extends java.lang.annotation.Annotation> scope) {
    Instance.Handle<PrincipalSource> handle = mock(Instance.Handle.class);
    Bean<PrincipalSource> bean = mock(Bean.class);
    when(instance.getHandle()).thenReturn(handle);
    when(handle.get()).thenReturn(source);
    when(handle.getBean()).thenReturn(bean);
    doReturn(scope).when(bean).getScope();
    return handle;
  }
}
