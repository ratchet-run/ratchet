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
package run.ratchet.ri.core.internal;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.RetryBufferDrainer;

class JobExecutionCoordinatorTest {
  @Test
  void releasesClaimsBeforeInterruptingWorkersThatOutliveDrain() throws InterruptedException {
    JobExecutorService executor = mock(JobExecutorService.class);
    JobStateManager state = mock(JobStateManager.class);
    RetryBufferDrainer drainer = mock(RetryBufferDrainer.class);
    CheckpointFlusher flusher = mock(CheckpointFlusher.class);
    JobExecutionCoordinator coordinator =
        new JobExecutionCoordinator(
            mock(JobSubmissionService.class), state, drainer, executor, flusher);
    when(executor.shutdownActiveExecutions()).thenReturn(2);

    coordinator.shutdown(Duration.ofSeconds(1));

    InOrder order = inOrder(drainer, executor, state, flusher);
    order.verify(drainer).shutdown();
    order.verify(executor).awaitIdle(Duration.ofSeconds(1));
    order.verify(flusher).flushAll();
    order.verify(state).resetRunningJobsForNode();
    order.verify(executor).shutdownActiveExecutions();
  }

  @Test
  void flushFailureDoesNotStopResetEvenWithZeroDrainTimeout() {
    CheckpointFlusher flusher = mock(CheckpointFlusher.class);
    JobStateManager state = mock(JobStateManager.class);
    JobExecutorService executor = mock(JobExecutorService.class);
    doThrow(new IllegalStateException("store failed")).when(flusher).flushAll();
    JobExecutionCoordinator coordinator =
        new JobExecutionCoordinator(
            mock(JobSubmissionService.class),
            state,
            mock(RetryBufferDrainer.class),
            executor,
            flusher);
    coordinator.shutdown(Duration.ZERO);
    InOrder order = inOrder(flusher, state, executor);
    order.verify(flusher).flushAll();
    order.verify(state).resetRunningJobsForNode();
    order.verify(executor).shutdownActiveExecutions();
  }
}
