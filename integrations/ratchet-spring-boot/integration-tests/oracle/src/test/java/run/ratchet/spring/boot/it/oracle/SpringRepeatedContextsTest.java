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
package run.ratchet.spring.boot.it.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.api.JobHandle;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.SpringRatchetTckRuntime;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.TckContexts;
import run.ratchet.tck.api.TckJobs;

/** Spring-local proof that the same consumer fixture can restart against one database. */
class SpringRepeatedContextsTest extends OracleIntegrationTestSupport {

  @Test
  void sameFixtureStartsAndRunsJobsTwiceAgainstOneDatabase() {
    runOneContext();
    runOneContext();
  }

  private static void runOneContext() {
    TckJobs.resetAll();
    try (ConfigurableApplicationContext context = TckContexts.start()) {
      SpringRatchetTckRuntime runtime = context.getBean(SpringRatchetTckRuntime.class);
      runtime.clear();

      JobHandle handle = runtime.scheduler().enqueueNow(TckJobs::noop);
      runtime.probe().track(handle);

      assertThat(runtime.probe().awaitCompleted(handle, Duration.ofSeconds(10))).isTrue();
    }
  }
}
