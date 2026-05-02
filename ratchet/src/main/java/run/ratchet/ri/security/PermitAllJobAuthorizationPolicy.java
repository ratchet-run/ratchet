package run.ratchet.ri.security;

import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.JobAuthorizationPolicy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.UUID;

/**
 * Default {@link JobAuthorizationPolicy} that permits every operation. Provides full
 * backward-compatibility — deployments without a custom policy see no behaviour change.
 *
 * <p>To enforce site-specific authorization, supply a CDI
 * {@code @Alternative @Priority(APPLICATION)} bean that implements {@link JobAuthorizationPolicy}.
 */
@ApplicationScoped
public class PermitAllJobAuthorizationPolicy implements JobAuthorizationPolicy {

  @Override
  public void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException {}

  @Override
  public void checkCancel(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkPause(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkResume(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkRetry(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}
}
