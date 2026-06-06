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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.UUID;
import run.ratchet.api.JobFilter;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.JobAuthorizationPolicy;

/**
 * {@link JobAuthorizationPolicy} {@code @Alternative} that permits job creation but denies every
 * read and scopes list queries to a principal no job carries. The TCK query contract uses it to
 * prove existence-hiding (a denied {@code getJobDetail} returns empty, indistinguishable from a
 * missing job) and {@code filterForPrincipal} scoping (a scoped {@code findJobs} returns nothing).
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class DenyReadJobAuthorizationPolicy implements JobAuthorizationPolicy {

  private static final String UNMATCHABLE_PRINCIPAL = "__denied-no-match__";

  @Override
  public void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException {}

  @Override
  public void checkRead(UUID jobId, String callerPrincipal) throws JobAuthorizationException {
    throw new JobAuthorizationException(jobId, "read", callerPrincipal, "Read denied by policy");
  }

  @Override
  public JobFilter filterForPrincipal(JobFilter filter, String callerPrincipal) {
    // Narrow to a principal no job carries, so list reads return nothing for this caller.
    return filter.toBuilder().callerPrincipal(UNMATCHABLE_PRINCIPAL).build();
  }

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
