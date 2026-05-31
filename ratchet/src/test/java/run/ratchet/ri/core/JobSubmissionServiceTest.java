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
package run.ratchet.ri.core;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class JobSubmissionServiceTest {

  @Mock private SubmissionGateChecker gateChecker;
  @Mock private JobExecutorService executorService;
  @Mock private SubmissionFailureHandler failureHandler;

  private JobSubmissionService service;

  @BeforeEach
  void setUp() {
    service = new JobSubmissionService(gateChecker, executorService, failureHandler);
  }

  @Test
  void submit_entityChecksFirstAttemptGateAndExecutes() {
    JobEntity job = singleJob();
    when(gateChecker.check(job, true)).thenReturn(GateCheckResult.clear("platform"));
    when(executorService.execute(job, "platform"))
        .thenReturn(ExecutionResult.success(completedFuture()));

    service.submit(job);

    verify(gateChecker).check(job, true);
    verify(executorService).execute(job, "platform");
    verify(failureHandler, never()).handleGateFailure(job, GateCheckResult.clear("platform"), true);
  }

  @Test
  void submitBuffered_entityChecksRetryGateAndPreservesRetryFailureHandling() {
    JobEntity job = singleJob();
    GateCheckResult gateResult = GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId());
    when(gateChecker.check(job, false)).thenReturn(gateResult);

    service.submitBuffered(job);

    verify(gateChecker).check(job, false);
    verify(failureHandler).handleGateFailure(job, gateResult, false);
    verify(executorService, never()).execute(job, "platform");
  }

  @Test
  void submitBuffered_claimChecksRetryGate() {
    // Buffered claims represent already-claimed work; the retry gate must apply (not the
    // first-attempt gate), matching the entity-overload contract.
    JobClaimDto claim = singleClaim();
    GateCheckResult gateResult = GateCheckResult.noPermits(JobExecutionType.SINGLE, claim.id());
    when(gateChecker.check(claim, false)).thenReturn(gateResult);

    service.submitBuffered(claim);

    verify(gateChecker).check(claim, false);
    verify(failureHandler).handleGateFailure(claim, gateResult);
    verify(executorService, never()).execute(claim, "platform");
  }

  private static JobEntity singleJob() {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(JobExecutionType.SINGLE);
    return job;
  }

  private static JobClaimDto singleClaim() {
    return new JobClaimDto(
        UUID.randomUUID(),
        JobStatus.RUNNING,
        JobExecutionType.SINGLE,
        null,
        null,
        0,
        30,
        "node-1",
        null,
        null,
        0,
        0,
        null);
  }

  private static CompletableFuture<Void> completedFuture() {
    return CompletableFuture.completedFuture(null);
  }
}
