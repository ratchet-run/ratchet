package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.AbstractJobAuthorizationContract;
import run.ratchet.tck.api.RatchetTckRuntime;

/**
 * RI subclass of {@link AbstractJobAuthorizationContract}.
 *
 * <p>The default deployment uses {@code PermitAllJobAuthorizationPolicy} (no {@code @Alternative}
 * override), so permit-all contract tests run end-to-end. Deny-all contract tests are skipped
 * because {@link #schedulerWithDenyAllPolicy()} returns empty — the denial scenario is covered
 * separately in {@link JobAuthorizationDenyIT}.
 */
@ExtendWith(ArquillianExtension.class)
class RiJobAuthorizationIT extends AbstractJobAuthorizationContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  @Test
  protected void denyAllPolicy_throwsJobAuthorizationExceptionOnSubmit() {
    // Arquillian mis-reports TestAbortedException as an error; covered by JobAuthorizationDenyIT.
  }

  @Override
  @Test
  protected void denyAllPolicy_nullPrincipalIsPassedThrough() {
    // Arquillian mis-reports TestAbortedException as an error; covered by JobAuthorizationDenyIT.
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create();
  }
}
