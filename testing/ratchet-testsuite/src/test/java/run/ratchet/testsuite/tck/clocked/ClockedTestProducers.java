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
package run.ratchet.testsuite.tck.clocked;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.interceptor.Interceptor;
import java.time.Clock;
import run.ratchet.tck.api.SteppingTestClock;
import run.ratchet.tck.api.TestClock;

/**
 * CDI producers that override the production {@link Clock} (from {@code RatchetProducer}) with a
 * single {@link SteppingTestClock} instance. Activated only inside the {@code
 * RiDelayedSchedulingIT} archive via its {@code beans-clocked.xml}; other Ri*IT deployments are
 * unaffected because the alternative is not declared in their {@code beans.xml}.
 */
@ApplicationScoped
public class ClockedTestProducers {

  private final SteppingTestClock instance = new SteppingTestClock();

  @Produces
  @Alternative
  @Priority(Interceptor.Priority.APPLICATION + 100)
  @ApplicationScoped
  public Clock testClock() {
    return instance;
  }

  /**
   * Producer for {@link TestClock}-typed injection points (e.g. {@link RiClockedTckRuntime#clock()}
   * needs {@code TestClock}, not just {@link Clock}). {@link Dependent} scope avoids the Weld
   * client proxy — an {@code @ApplicationScoped} proxy of {@code TestClock} would lose the {@code
   * Clock} interface and vice-versa, breaking either the production-side {@code @Inject Clock} or
   * the test-side {@code @Inject TestClock}. Returning the same singleton instance from a {@code
   * Dependent} producer keeps both injections pointing at one underlying clock.
   */
  @Produces
  @Dependent
  public TestClock testClockTyped() {
    return instance;
  }
}
