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
package run.ratchet.tck.jakarta;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * Base contract: a Jakarta-EE-compliant Ratchet runtime MUST resolve {@code @Inject
 * JobSchedulerService}, and the resolved instance MUST be functionally equivalent to one obtained
 * via the runtime's bootstrap. Both are exercised — bare resolution is not enough on its own.
 *
 * <p>Subclasses provide:
 *
 * <ul>
 *   <li>{@code @ExtendWith(ArquillianExtension.class)} on the class.
 *   <li>A {@code @Deployment} producing a WebArchive that bundles this contract package and the
 *       implementation's {@link RatchetTckRuntime} adapter.
 *   <li>An override of {@link #runtime()} that returns the implementation-specific runtime
 *       (typically {@code @Inject}-ed).
 * </ul>
 */
public abstract class AbstractCdiInjectionContract {

  /**
   * Container-injected scheduler. The presence of this field after Arquillian field-enrichment is
   * the primary assertion of this contract — if CDI cannot resolve {@link JobSchedulerService},
   * Arquillian fails before any test method runs.
   */
  @Inject protected JobSchedulerService injectedScheduler;

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void injectResolvesScheduler() {
    assertNotNull(
        injectedScheduler,
        "Jakarta runtime MUST publish JobSchedulerService as an injectable bean. "
            + "Arquillian field-enrichment runs before this test — a null here means the "
            + "implementation did not register a producer / managed bean for the API interface.");
  }

  @Test
  void injectedSchedulerExecutesJob() {
    JobHandle handle = injectedScheduler.enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job submitted via @Inject-ed scheduler must complete. Bare CDI resolution is not enough "
            + "— the resolved bean must be a working scheduler, not a no-op stub or a producer "
            + "for an unstarted scheduler.");
  }

  /** Runtime under test. Subclasses typically inject and return an implementation-specific bean. */
  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }
}
