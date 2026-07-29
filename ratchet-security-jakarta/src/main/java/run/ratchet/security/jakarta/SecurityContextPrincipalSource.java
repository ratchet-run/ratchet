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
package run.ratchet.security.jakarta;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Vetoed;
import jakarta.security.enterprise.SecurityContext;
import java.security.Principal;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.spi.PrincipalSource;
import run.ratchet.spi.PrincipalSourceInstances;

/**
 * Jakarta Security-backed {@link PrincipalSource}.
 *
 * <p>Plain-CDI and non-EE runtimes often have no {@link SecurityContext} bean. This source treats
 * an absent or failing context as empty so caller-principal capture never blocks job creation.
 */
@Vetoed
public class SecurityContextPrincipalSource implements PrincipalSource {

  private static final Logger log = Logger.getLogger(SecurityContextPrincipalSource.class);

  private final Instance<SecurityContext> securityContexts;

  protected SecurityContextPrincipalSource() {
    this.securityContexts = null;
  }

  public SecurityContextPrincipalSource(Instance<SecurityContext> securityContexts) {
    this.securityContexts = securityContexts;
  }

  @Override
  public Optional<String> currentPrincipal() {
    return PrincipalSourceInstances.currentPrincipal(
        securityContexts,
        SecurityContextPrincipalSource::principalName,
        "SecurityContext Instance was not injected",
        e -> log.warnf(e, "SecurityContext lookup failed; capturing null caller principal"));
  }

  private static Optional<String> principalName(SecurityContext context) {
    Principal principal = context.getCallerPrincipal();
    return principal == null ? Optional.empty() : Optional.ofNullable(principal.getName());
  }
}
