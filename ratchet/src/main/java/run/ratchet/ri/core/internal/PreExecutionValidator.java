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
