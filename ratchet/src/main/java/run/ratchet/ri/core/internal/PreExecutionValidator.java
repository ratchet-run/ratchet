package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.store.entity.JobPayload;

@ApplicationScoped
public class PreExecutionValidator {

  private final JobSecurityValidator securityValidator;
  private final DoNotRetryPolicy doNotRetryPolicy;

  protected PreExecutionValidator() {
    this.securityValidator = null;
    this.doNotRetryPolicy = null;
  }

  @Inject
  public PreExecutionValidator(
      JobSecurityValidator securityValidator, DoNotRetryPolicy doNotRetryPolicy) {
    this.securityValidator = securityValidator;
    this.doNotRetryPolicy = doNotRetryPolicy;
  }

  public void validateSecurity(JobPayload payload) {
    securityValidator.validate(payload);
  }

  public boolean shouldNotRetry(Throwable ex) {
    return doNotRetryPolicy.shouldNotRetry(ex);
  }
}
