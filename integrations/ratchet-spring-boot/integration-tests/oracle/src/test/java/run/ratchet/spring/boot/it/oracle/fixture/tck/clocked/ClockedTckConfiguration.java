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
package run.ratchet.spring.boot.it.oracle.fixture.tck.clocked;

import java.time.Clock;
import java.util.Set;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.spring.boot.it.oracle.fixture.tck.TckConfiguration;
import run.ratchet.tck.api.ListenerProbe;
import run.ratchet.tck.api.SteppingTestClock;
import run.ratchet.tck.store.clocked.InMemoryJobStore;

/** Beans replacing Oracle time and storage for the clock-dependent contract. */
@Configuration(proxyBeanMethods = false)
public class ClockedTckConfiguration {

  @Bean
  RatchetOptions clockedTckRatchetOptions() {
    return RatchetOptions.builder()
        .security(
            security ->
                security.classPolicyAllowedPackages(
                    Set.of(TckConfiguration.APPLICATION_PACKAGE, TckConfiguration.TCK_PACKAGE)))
        .build();
  }

  @Bean
  @Primary
  SteppingTestClock steppingTestClock() {
    return new SteppingTestClock();
  }

  @Bean
  @Primary
  InMemoryJobStore clockedJobStore(Clock clock) {
    return new InMemoryJobStore(clock);
  }

  @Bean
  ListenerProbe listenerProbe() {
    return new ListenerProbe();
  }

  @Bean
  SpringClockedTckRuntime springClockedTckRuntime(
      JobSchedulerService scheduler,
      ListenerProbe probe,
      DrainController drainController,
      JobExecutorService executor,
      SteppingTestClock clock,
      InMemoryJobStore jobStore) {
    return new SpringClockedTckRuntime(
        scheduler, probe, drainController, executor, clock, jobStore);
  }
}
