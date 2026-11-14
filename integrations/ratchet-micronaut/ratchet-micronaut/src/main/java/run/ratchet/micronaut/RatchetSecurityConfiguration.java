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
package run.ratchet.micronaut;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import run.ratchet.spi.PrincipalSource;

/** An application's PrincipalSource bean replaces this security bridge. */
@Factory
@Requires(classes = SecurityService.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
class RatchetSecurityConfiguration {
  @Singleton
  @Requires(beans = SecurityService.class)
  @Requires(missingBeans = PrincipalSource.class)
  PrincipalSource securityPrincipalSource(SecurityService security) {
    return new MicronautSecurityPrincipalSource(security);
  }
}
