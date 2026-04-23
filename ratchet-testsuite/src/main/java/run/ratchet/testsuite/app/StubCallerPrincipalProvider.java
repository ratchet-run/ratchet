package run.ratchet.testsuite.app;

import run.ratchet.ri.security.CallerPrincipalProvider;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Optional;

/**
 * {@link CallerPrincipalProvider} {@code @Alternative} that returns a fixed principal without
 * needing a container security realm. Used by {@code CallerPrincipalCaptureIT} to prove that the
 * framework stamps the captured principal onto the persisted {@code JobEntity} at creation — the
 * realm-configured Arquillian deployment is deferred follow-up work.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class StubCallerPrincipalProvider extends CallerPrincipalProvider {

  public static final String STUB_PRINCIPAL = "it-caller";

  public StubCallerPrincipalProvider() {
    super();
  }

  @Override
  public Optional<String> currentPrincipal() {
    return Optional.of(STUB_PRINCIPAL);
  }
}
