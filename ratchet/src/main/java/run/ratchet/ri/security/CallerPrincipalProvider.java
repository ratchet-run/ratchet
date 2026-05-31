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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.security.enterprise.SecurityContext;
import java.security.Principal;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Resolves the current caller principal from {@link SecurityContext} when one is available in the
 * container, and returns an empty Optional otherwise. Stamped onto {@code
 * JobEntity.callerPrincipal} at job creation for audit.
 *
 * <p>The captured principal is also passed to the pluggable {@link
 * run.ratchet.spi.JobAuthorizationPolicy} SPI at every mutation point ({@code checkCreate}, {@code
 * checkCancel}, {@code checkPause}, {@code checkResume}, {@code checkRetry}, {@code
 * checkDeliverSignal}) and at read points ({@code checkRead}, {@code filterForPrincipal}). The
 * default implementation, {@code PermitAllJobAuthorizationPolicy}, permits all operations;
 * applications override it with a CDI {@code @Alternative @Priority(APPLICATION)} bean to enforce
 * site-specific authorization.
 *
 * <p>In plain-CDI / Weld SE test environments no {@code SecurityContext} implementation is
 * resolvable — the provider silently returns empty rather than failing. This keeps the RI usable
 * outside a full EE profile.
 *
 * @see run.ratchet.api.JobSchedulerService for the normative capture contract
 * @see run.ratchet.spi.JobAuthorizationPolicy for the enforcement SPI
 */
@ApplicationScoped
public class CallerPrincipalProvider {

  private static final Logger log = Logger.getLogger(CallerPrincipalProvider.class);

  private final Instance<SecurityContext> securityContexts;

  protected CallerPrincipalProvider() {
    this.securityContexts = null;
  }

  @Inject
  public CallerPrincipalProvider(Instance<SecurityContext> securityContexts) {
    this.securityContexts = securityContexts;
  }

  /**
   * Returns the authenticated caller principal name when a {@link SecurityContext} is resolvable
   * and holds an authenticated principal. Returns empty when no context is available or the context
   * reports no principal (unauthenticated request, or non-EE runtime).
   */
  public Optional<String> currentPrincipal() {
    if (securityContexts == null) {
      throw new IllegalStateException("SecurityContext Instance was not injected");
    }
    try {
      if (!securityContexts.isResolvable()) {
        return Optional.empty();
      }
      Instance.Handle<SecurityContext> handle = securityContexts.getHandle();
      try {
        Principal principal = handle.get().getCallerPrincipal();
        if (principal == null) {
          return Optional.empty();
        }
        String name = principal.getName();
        return (name == null || name.isEmpty()) ? Optional.empty() : Optional.of(name);
      } finally {
        if (handle.getBean().getScope().equals(Dependent.class)) {
          handle.destroy();
        }
      }
    } catch (RuntimeException e) {
      log.warnf(e, "SecurityContext lookup failed; capturing null caller principal");
      return Optional.empty();
    }
  }
}
