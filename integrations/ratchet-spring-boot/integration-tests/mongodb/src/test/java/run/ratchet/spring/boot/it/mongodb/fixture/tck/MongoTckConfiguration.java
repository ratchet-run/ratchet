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
package run.ratchet.spring.boot.it.mongodb.fixture.tck;

import com.mongodb.client.MongoDatabase;
import java.util.Set;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.ListenerProbe;

/** Spring beans adapting the shared API TCK to the MongoDB-backed runtime. */
@Configuration(proxyBeanMethods = false)
public class MongoTckConfiguration {

  public static final String APPLICATION_PACKAGE = "run.ratchet.spring.boot.it.mongodb";
  public static final String TCK_PACKAGE = "run.ratchet.tck.api";

  @Bean
  RatchetOptions mongoTckRatchetOptions() {
    return RatchetOptions.builder()
        .security(
            security ->
                security.classPolicyAllowedPackages(Set.of(APPLICATION_PACKAGE, TCK_PACKAGE)))
        .build();
  }

  @Bean
  ListenerProbe listenerProbe() {
    return new ListenerProbe();
  }

  @Bean
  MongoTckStoreCleaner mongoTckStoreCleaner(MongoDatabase database) {
    return new MongoTckStoreCleaner(database);
  }

  @Bean
  SpringMongoRatchetTckRuntime springMongoRatchetTckRuntime(
      JobSchedulerService scheduler,
      ListenerProbe probe,
      DrainController drainController,
      JobExecutorService executor,
      MongoTckStoreCleaner storeCleaner,
      RatchetOptions options) {
    return new SpringMongoRatchetTckRuntime(
        scheduler, probe, drainController, executor, storeCleaner, options);
  }
}
