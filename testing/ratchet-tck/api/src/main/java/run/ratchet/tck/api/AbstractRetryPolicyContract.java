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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for a custom {@link run.ratchet.spi.RetryPolicy} override.
 *
 * <p>{@code shouldRetry} is ANDed with the job's {@code maxRetries}: a policy can stop retries
 * early but cannot push them past the configured ceiling. This contract installs a policy that
 * vetoes at attempt {@link #vetoAttempt()} and submits a deterministically-failing job whose {@code
 * maxRetries} is set strictly higher, then asserts the body ran exactly {@code vetoAttempt} times —
 * proving the policy, not the ceiling, ended the retries.
 *
 * <p>Runtimes that cannot install a custom SPI bean in a test context return {@link
 * Optional#empty()} from {@link #retryPolicyRuntime()} and the contract is reported not-applicable,
 * mirroring the optional deny-all hook on {@link AbstractJobAuthorizationContract}.
 */
public abstract class AbstractRetryPolicyContract {

  @AfterEach
  void clearAfterEach() {
    retryPolicyRuntime().ifPresent(RatchetTckRuntime::clear);
    TckJobs.resetAll();
  }

  @Test
  void customPolicyTerminatesRetriesBelowMaxRetries() {
    Optional<RatchetTckRuntime> maybe = retryPolicyRuntime();
    assumeTrue(
        maybe.isPresent(), "runtime cannot install a custom RetryPolicy in this test context");
    RatchetTckRuntime runtime = maybe.get();

    int veto = vetoAttempt();
    int maxRetries = veto + 5; // strictly above the veto, so the ceiling is never the limiter

    JobHandle handle =
        runtime.scheduler().enqueue(TckJobs::throwIntentional).withMaxRetries(maxRetries).submit();
    runtime.probe().track(handle);

    assertTrue(
        runtime.probe().awaitFailed(handle, defaultTimeout()),
        "the job must reach terminal FAILED once the policy stops retrying");
    assertEquals(
        veto,
        runtime.probe().invocationCount(handle),
        "the custom policy must cap the body at "
            + veto
            + " runs, well below maxRetries+1 ("
            + (maxRetries + 1)
            + "); the engine ANDs shouldRetry with maxRetries");
  }

  /**
   * A runtime whose injected {@link run.ratchet.spi.RetryPolicy} returns {@code true} from {@code
   * shouldRetry} only while the attempt is below {@link #vetoAttempt()}. Empty when the runtime
   * cannot swap the SPI in a test context.
   */
  protected abstract Optional<RatchetTckRuntime> retryPolicyRuntime();

  /**
   * The 1-based attempt at which the installed policy first vetoes a retry. Equals the number of
   * times the body runs. Subclasses override this to match the policy they install.
   */
  protected int vetoAttempt() {
    return 2;
  }

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }
}
