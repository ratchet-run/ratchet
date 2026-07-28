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
package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/**
 * Application-supplied seam for resolving the current caller principal through configuration the
 * application already controls, bypassing CDI {@code @Alternative} bean resolution entirely.
 *
 * <p>Configured via {@code RatchetOptions.Builder#callerPrincipalResolver}. When set, it takes
 * first precedence in the reference implementation's resolution cascade. If this resolver returns
 * {@link Optional#empty()}, returns {@code null}, or throws a {@link RuntimeException}, Ratchet
 * treats this source as empty and continues to the next source: the executing {@code JobContext}'s
 * captured caller principal, then {@code run.ratchet.ri.security.CallerPrincipalProvider}, then no
 * principal. Some EAR containers silently ignore an application's
 * {@code @Alternative @Priority(APPLICATION) CallerPrincipalProvider} override, so this seam exists
 * as a path that does not depend on CDI alternative resolution at all.
 *
 * <p>Ratchet never invents a principal. A persisted {@code null} {@code caller_principal} means no
 * principal was captured, such as a background or system-initiated submission. Applications that
 * want a literal stamp such as {@code "system"} must return that value from this resolver.
 *
 * <h2>Authentication mechanism and the default capture</h2>
 *
 * <p>The reference implementation reads platform identities through {@link PrincipalSource}. The
 * Jakarta EE carrier supplies a source backed by {@code
 * jakarta.security.enterprise.SecurityContext}, which a container registers only when the
 * deployment activates Jakarta Security — an {@code HttpAuthenticationMechanism}, {@code
 * IdentityStore}, or a {@code @…AuthenticationMechanismDefinition}. Some integrations authenticate
 * the caller fully without activating Jakarta Security, so no {@code SecurityContext} bean is
 * registered and the Jakarta EE source records no principal even for an authenticated request.
 * WildFly's native Elytron OIDC ({@code web.xml} {@code auth-method=OIDC} via the {@code
 * elytron-oidc-client} subsystem) is one such case: {@code HttpServletRequest.getUserPrincipal()}
 * and {@code @RolesAllowed} work, but {@code SecurityContext} is never resolvable. Under such a
 * mechanism, supply this resolver and read an identity source the mechanism actually populates —
 * {@code HttpServletRequest.getUserPrincipal()} or the Elytron {@code SecurityIdentity} — rather
 * than relying on the Jakarta Security source.
 *
 * <h2>Proxy discipline</h2>
 *
 * <p>{@link #resolve()} is invoked per submission or authorization check, but the {@code
 * CallerPrincipalResolver} instance itself is captured only <strong>once</strong> — when the
 * application's {@code @ApplicationScoped RatchetOptions} producer method runs. An application MUST
 * close this functional interface over a normal-scoped CDI proxy (or call {@code Instance<T>.get()}
 * from inside {@link #resolve()} on every invocation), and MUST NOT close over a directly-injected
 * {@code @Dependent} reference. A {@code @Dependent} reference is resolved once, at the moment the
 * producer method runs, and would freeze whatever principal was live at container start —
 * reproducing the exact bug this seam exists to fix.
 *
 * <p>{@code PrincipalSource} implementations are the worked examples of the correct pattern: hold
 * an {@code Instance<MyRequestScopedActor>} and call {@code Instance.getHandle()}/{@code get()}
 * inside {@code currentPrincipal()} on every invocation, rather than capturing a directly injected
 * request-scoped identity. An application's resolver should follow the same shape — inject {@code
 * Instance<MyRequestScopedActor>} into its producer and call {@code .get()} from inside the lambda
 * passed to {@code callerPrincipalResolver(...)}.
 */
@Incubating
@FunctionalInterface
public interface CallerPrincipalResolver {

  /**
   * Resolves the current caller principal.
   *
   * <p>May be invoked from a background or materializer thread (chain-step continuation,
   * workflow-branch creation, batch-child creation) where a request-scoped proxy is not active. In
   * that situation, prefer returning {@link Optional#empty()}; a thrown {@link RuntimeException} is
   * also tolerated by the framework's resolution helper, which logs a warning, treats this source
   * as empty, and continues the resolution cascade rather than failing the submission.
   *
   * @return the resolved principal name, or {@link Optional#empty()} if none is available
   */
  Optional<String> resolve();
}
