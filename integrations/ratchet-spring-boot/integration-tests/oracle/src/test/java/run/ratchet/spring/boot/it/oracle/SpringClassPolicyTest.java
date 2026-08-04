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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.TckContexts;
import run.ratchet.tck.api.TckJobs;

/** Spring-local proof that the fixture's explicit package allowlist rejects foreign targets. */
class SpringClassPolicyTest extends OracleIntegrationTestSupport {

  @Test
  void dualWiredClassPolicyAllowsFixtureJobsAndRejectsJdkTargets() {
    try (ConfigurableApplicationContext context = TckContexts.start()) {
      ClassPolicy classPolicy = context.getBean(ClassPolicy.class);
      JobSchedulerService scheduler = context.getBean(JobSchedulerService.class);

      assertThat(classPolicy.isAllowed(TckJobs.class.getName())).isTrue();
      assertThat(classPolicy.isAllowed(SpringClassPolicyTest.class.getName())).isTrue();
      assertThat(classPolicy.isAllowed(System.class.getName())).isFalse();
      assertThatThrownBy(() -> scheduler.enqueueNow(System::gc))
          .isInstanceOfAny(SecurityException.class, IllegalArgumentException.class);
    }
  }
}
