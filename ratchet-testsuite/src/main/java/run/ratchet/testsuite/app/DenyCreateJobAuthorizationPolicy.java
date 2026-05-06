package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.UUID;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.JobAuthorizationPolicy;

/**
 * {@link JobAuthorizationPolicy} {@code @Alternative} that denies all job creation attempts. Used
 * in IT tests to verify that {@link JobAuthorizationException} propagates correctly from the
 * scheduler to callers when creation is blocked.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class DenyCreateJobAuthorizationPolicy implements JobAuthorizationPolicy {

  @Override
  public void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException {
    throw new JobAuthorizationException(
        jobId, "create", callerPrincipal, "Creation denied by policy");
  }

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
