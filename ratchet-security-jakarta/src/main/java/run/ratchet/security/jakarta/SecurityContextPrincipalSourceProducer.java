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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.security.enterprise.SecurityContext;

/** Produces the default Jakarta Security principal source for Jakarta EE runtimes. */
@ApplicationScoped
public class SecurityContextPrincipalSourceProducer {

  @Produces
  @Default
  @ApplicationScoped
  public SecurityContextPrincipalSource securityContextPrincipalSource(
      Instance<SecurityContext> securityContexts) {
    return new SecurityContextPrincipalSource(securityContexts);
  }
}
