package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import jakarta.security.enterprise.SecurityContext;
import java.security.Principal;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CallerPrincipalProviderTest {

  private static final class UnmanagedCallerPrincipalProvider extends CallerPrincipalProvider {}

  @Test
  void currentPrincipal_resolvableAuthenticated_returnsName() {
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("alice");
    SecurityContext context = mock(SecurityContext.class);
    when(context.getCallerPrincipal()).thenReturn(principal);

    @SuppressWarnings("unchecked")
    Instance<SecurityContext> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(context);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isPresent(), "Authenticated principal should be captured");
    assertEquals("alice", result.get());
  }

  @Test
  void currentPrincipal_resolvableUnauthenticated_returnsEmpty() {
    SecurityContext context = mock(SecurityContext.class);
    when(context.getCallerPrincipal()).thenReturn(null);

    @SuppressWarnings("unchecked")
    Instance<SecurityContext> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(context);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Null principal on context should yield empty");
  }

  @Test
  void currentPrincipal_notResolvable_returnsEmpty() {
    @SuppressWarnings("unchecked")
    Instance<SecurityContext> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(false);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Unresolvable SecurityContext should yield empty");
  }

  @Test
  void currentPrincipal_securityContextLookupFailure_returnsEmpty() {
    @SuppressWarnings("unchecked")
    Instance<SecurityContext> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenThrow(new IllegalStateException("container is shutting down"));

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "SecurityContext failures should not block job creation");
  }

  @Test
  void currentPrincipal_getCallerPrincipalFailure_returnsEmpty() {
    SecurityContext context = mock(SecurityContext.class);
    when(context.getCallerPrincipal()).thenThrow(new RuntimeException("security context failed"));

    @SuppressWarnings("unchecked")
    Instance<SecurityContext> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(context);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Principal lookup failures should not block job creation");
  }

  @Test
  void currentPrincipal_unmanagedNoArgConstruction_throws() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> new UnmanagedCallerPrincipalProvider().currentPrincipal());

    assertEquals("SecurityContext Instance was not injected", exception.getMessage());
  }

  @Test
  void currentPrincipal_emptyPrincipalName_returnsEmpty() {
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("");
    SecurityContext context = mock(SecurityContext.class);
    when(context.getCallerPrincipal()).thenReturn(principal);

    @SuppressWarnings("unchecked")
    Instance<SecurityContext> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(context);

    Optional<String> result = new CallerPrincipalProvider(instance).currentPrincipal();

    assertTrue(result.isEmpty(), "Empty principal name should yield empty");
  }
}
