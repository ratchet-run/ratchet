package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import run.ratchet.store.entity.JobPayload;

@ApplicationScoped
public class PreExecutionValidator {

  private final SecurityValidator securityValidator;
  private final DoNotRetryPolicy doNotRetryPolicy;

  protected PreExecutionValidator() {
    this.securityValidator = null;
    this.doNotRetryPolicy = null;
  }

  @Inject
  public PreExecutionValidator(
      SecurityValidator securityValidator, DoNotRetryPolicy doNotRetryPolicy) {
    this.securityValidator = securityValidator;
    this.doNotRetryPolicy = doNotRetryPolicy;
  }

  public void validateSecurity(JobPayload payload) {
    securityValidator.validate(payload);
  }

  public boolean shouldNotRetry(Throwable ex) {
    return doNotRetryPolicy.shouldNotRetry(ex);
  }

  @FunctionalInterface
  public interface SecurityValidator {
    void validate(JobPayload payload);
  }

  @FunctionalInterface
  public interface DoNotRetryPolicy {
    boolean shouldNotRetry(Throwable ex);
  }
}
