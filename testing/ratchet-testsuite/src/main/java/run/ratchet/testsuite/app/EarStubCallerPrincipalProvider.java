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
package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor.Priority;
import java.util.Optional;
import run.ratchet.ri.security.CallerPrincipalProvider;

/**
 * {@link CallerPrincipalProvider} {@code @Alternative} used from an EAR EJB-jar subdeployment to
 * verify that application-level alternatives override Ratchet's default provider across
 * subdeployment boundaries.
 */
@Alternative
@jakarta.annotation.Priority(Priority.APPLICATION)
@ApplicationScoped
public class EarStubCallerPrincipalProvider extends CallerPrincipalProvider {

  public static final String STUB_PRINCIPAL = "ear-it-caller";

  public EarStubCallerPrincipalProvider() {
    super();
  }

  @Override
  public Optional<String> currentPrincipal() {
    return Optional.of(STUB_PRINCIPAL);
  }
}
