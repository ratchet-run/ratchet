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

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Vetoed;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.spi.PrincipalSource;
import run.ratchet.spi.PrincipalSourceInstances;

/**
 * Resolves the current caller principal from a platform {@link PrincipalSource}, and returns an
 * empty Optional otherwise. Stamped onto {@code JobEntity.callerPrincipal} at job creation for
 * audit.
 *
 * <p>The provider is the third source in the reference implementation's caller-principal cascade:
 * configured {@code run.ratchet.spi.CallerPrincipalResolver}, bound {@code JobContext} caller
 * principal, this provider, then empty. The bound {@code JobContext} source intentionally outranks
 * this provider so jobs that submit children on their execution thread inherit the parent's
 * persisted principal even when an application provider falls back to a worker-thread service
 * account.
 *
 * <p>Ratchet never invents a principal. A persisted {@code null} {@code caller_principal} means no
 * principal was captured, such as a background or system-initiated submission. Applications that
 * want a literal stamp such as {@code "system"} must supply it through {@code
 * run.ratchet.spi.CallerPrincipalResolver} or an overriding provider.
 *
 * <p>The default bean is supplied by a {@code @Produces @Default} producer on {@link
 * run.ratchet.ri.cdi.RatchetProducer}. Applications override it with a CDI
 * {@code @Alternative @Priority(APPLICATION) CallerPrincipalProvider} bean.
 *
 * <p>The captured principal is also passed to the pluggable {@link
 * run.ratchet.spi.JobAuthorizationPolicy} SPI at every mutation point ({@code checkCreate}, {@code
 * checkCancel}, {@code checkPause}, {@code checkResume}, {@code checkRetry}, {@code
 * checkDeliverSignal}) and at read points ({@code checkRead}, {@code filterForPrincipal}). The
 * default implementation, {@code PermitAllJobAuthorizationPolicy}, permits all operations;
 * applications override it with a CDI {@code @Alternative @Priority(APPLICATION)} bean to enforce
 * site-specific authorization.
 *
 * <p>In plain-CDI / Weld SE test environments no {@code PrincipalSource} implementation may be
 * resolvable — the provider silently returns empty rather than failing. This keeps the RI usable
 * outside a full EE profile.
 *
 * @see run.ratchet.api.JobSchedulerService for the normative capture contract
 * @see run.ratchet.spi.JobAuthorizationPolicy for the enforcement SPI
 */
@Vetoed
public class CallerPrincipalProvider {

  private static final Logger log = Logger.getLogger(CallerPrincipalProvider.class);

  private final Instance<PrincipalSource> sources;
  private final List<PrincipalSource> orderedSources;

  protected CallerPrincipalProvider() {
    this.sources = null;
    this.orderedSources = null;
  }

  public CallerPrincipalProvider(Instance<PrincipalSource> sources) {
    this.sources = sources;
    this.orderedSources = null;
  }

  /** Creates a provider that consults Spring or another container's ordered source collection. */
  public CallerPrincipalProvider(List<PrincipalSource> sources) {
    Objects.requireNonNull(sources, "sources must not be null");
    this.sources = null;
    this.orderedSources = Collections.unmodifiableList(new ArrayList<>(sources));
  }

  /**
   * Returns the authenticated caller principal name when a {@link PrincipalSource} is resolvable
   * and reports one. Returns empty when no source is available or the source reports no principal
   * (unauthenticated request, or non-EE runtime).
   */
  public Optional<String> currentPrincipal() {
    if (orderedSources != null) {
      return PrincipalSourceInstances.currentPrincipal(
          orderedSources,
          CallerPrincipalProvider::sourcePrincipal,
          e -> log.warnf(e, "PrincipalSource lookup failed; trying the next source"));
    }
    return PrincipalSourceInstances.currentPrincipal(
        sources,
        CallerPrincipalProvider::sourcePrincipal,
        "PrincipalSource Instance was not injected",
        e -> log.warnf(e, "PrincipalSource lookup failed; capturing null caller principal"));
  }

  private static Optional<String> sourcePrincipal(PrincipalSource source) {
    return source == null ? Optional.empty() : source.currentPrincipal();
  }
}
