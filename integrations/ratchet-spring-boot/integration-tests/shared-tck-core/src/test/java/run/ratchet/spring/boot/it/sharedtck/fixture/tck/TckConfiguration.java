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
package run.ratchet.spring.boot.it.sharedtck.fixture.tck;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.ListenerProbe;

/** Spring beans adapting the shared API TCK to the selected store runtime. */
@Configuration(proxyBeanMethods = false)
public class TckConfiguration {

  public static final String APPLICATION_PACKAGE = "run.ratchet.spring.boot.it.sharedtck";
  public static final String TCK_PACKAGE = "run.ratchet.tck.api";

  @Bean
  StoreTckBinding storeTckBinding() {
    return StoreTckBindings.binding();
  }

  @Bean
  RatchetOptions tckRatchetOptions(StoreTckBinding binding) {
    RatchetOptions.Builder builder =
        RatchetOptions.builder()
            .security(security -> security.classPolicyAllowedPackages(binding.allowedPackages()));
    binding
        .migrationDialect()
        .ifPresent(
            dialect ->
                builder.schema(schema -> schema.autoMigrate(true).migrationDialect(dialect)));
    return builder.build();
  }

  @Bean
  ListenerProbe listenerProbe() {
    return new ListenerProbe();
  }

  @Bean
  SpringRatchetTckRuntime springRatchetTckRuntime(
      JobSchedulerService scheduler,
      ListenerProbe probe,
      DrainController drainController,
      JobExecutorService executor,
      StoreTckBinding binding,
      RatchetOptions options) {
    return new SpringRatchetTckRuntime(scheduler, probe, drainController, executor, binding, options);
  }
}
