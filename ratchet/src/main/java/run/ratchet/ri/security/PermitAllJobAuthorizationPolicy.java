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
import java.util.UUID;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.JobAuthorizationPolicy;

/**
 * Default {@link JobAuthorizationPolicy} that permits every operation. Provides full
 * backward-compatibility — deployments without a custom policy see no behaviour change.
 *
 * <p>To enforce site-specific authorization, supply a CDI
 * {@code @Alternative @Priority(APPLICATION)} bean that implements {@link JobAuthorizationPolicy}.
 */
@ApplicationScoped
public class PermitAllJobAuthorizationPolicy implements JobAuthorizationPolicy {

  @Override
  public void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException {}

  @Override
  public void checkCancel(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkPause(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkResume(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  @Override
  public void checkRetry(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}
}
