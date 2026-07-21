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

import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.spi.CallerPrincipalResolver;

/**
 * Central precedence rule for caller-principal resolution: an application-supplied {@link
 * CallerPrincipalResolver} (read from {@code RatchetOptions#callerPrincipalResolver()}) takes
 * precedence over the CDI-alternative-based {@link CallerPrincipalProvider} default. Every call
 * site that needs the current caller principal goes through {@link #resolve} rather than
 * re-implementing this precedence and its safety net inline.
 */
public final class CallerPrincipalResolution {

  private static final Logger log = Logger.getLogger(CallerPrincipalResolution.class);

  private CallerPrincipalResolution() {}

  /**
   * Resolves the current caller principal, preferring {@code optionResolver} when one is
   * configured.
   *
   * <p>{@code optionResolver} may be invoked off a background or materializer thread (chain-step
   * continuation, workflow-branch creation, batch-child creation) where a request-scoped proxy is
   * not active and throws (for example {@code ContextNotActiveException}). Any {@link
   * RuntimeException} it throws — and a {@code null} {@link Optional} return — degrade to {@link
   * Optional#empty()} here rather than failing the submission or authorization check.
   *
   * @param optionResolver the application-supplied resolver from {@code
   *     RatchetOptions#callerPrincipalResolver()}; may be {@code null} when unconfigured
   * @param provider the CDI-alternative-based default provider; may be {@code null} in test wiring
   * @return the resolved principal, or {@link Optional#empty()} if neither source yields one
   */
  public static Optional<String> resolve(
      CallerPrincipalResolver optionResolver, CallerPrincipalProvider provider) {
    if (optionResolver != null) {
      try {
        Optional<String> resolved = optionResolver.resolve();
        return resolved != null ? resolved : Optional.empty();
      } catch (RuntimeException e) {
        log.warnf(e, "CallerPrincipalResolver threw; capturing null caller principal");
        return Optional.empty();
      }
    }
    return provider != null ? provider.currentPrincipal() : Optional.empty();
  }
}
