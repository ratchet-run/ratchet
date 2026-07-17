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
 * precedence over {@code run.ratchet.ri.security.CallerPrincipalProvider} — the reference
 * implementation's CDI-alternative-based default. Leaving it unset reproduces today's behavior
 * exactly: some EAR containers silently ignore an application's
 * {@code @Alternative @Priority(APPLICATION) CallerPrincipalProvider} override, so this seam exists
 * as a fallback path that does not depend on CDI alternative resolution at all.
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
 * <p>{@code run.ratchet.ri.security.CallerPrincipalProvider} is the worked example of the correct
 * pattern: it holds an {@code Instance<SecurityContext>} and calls {@code
 * Instance.getHandle()}/{@code get()} inside its own {@code currentPrincipal()} method on every
 * invocation, rather than injecting {@code SecurityContext} directly. An application's resolver
 * should follow the same shape — inject {@code Instance<MyRequestScopedActor>} into its producer
 * and call {@code .get()} from inside the lambda passed to {@code callerPrincipalResolver(...)}.
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
   * also tolerated by the framework's resolution helper, which logs a warning and degrades to
   * {@link Optional#empty()} rather than failing the submission.
   *
   * @return the resolved principal name, or {@link Optional#empty()} if none is available
   */
  Optional<String> resolve();
}
