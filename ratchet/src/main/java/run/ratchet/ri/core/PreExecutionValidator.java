package run.ratchet.ri.core;

import run.ratchet.store.entity.JobPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

  public void validateSecurity(JobPayload payload) throws NoSuchMethodException {
    securityValidator.validate(payload);
  }

  public boolean shouldNotRetry(Throwable ex) {
    return doNotRetryPolicy.shouldNotRetry(ex);
  }

  @FunctionalInterface
  public interface SecurityValidator {
    void validate(JobPayload payload) throws NoSuchMethodException;
  }

  @FunctionalInterface
  public interface DoNotRetryPolicy {
    boolean shouldNotRetry(Throwable ex);
  }
}
