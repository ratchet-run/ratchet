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
package run.ratchet.micronaut;

import static org.mockito.Mockito.mock;

import io.micronaut.context.ApplicationContext;
import java.util.HashMap;
import java.util.Map;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.store.spi.JobStore;

/** Context fixtures register mocks before starting, so real worker threads never start. */
final class TestContexts {
  private TestContexts() {}

  static ApplicationContext core(Map<String, Object> properties, Object... singletons) {
    Map<String, Object> values = new HashMap<>(properties);
    values.putIfAbsent("ratchet.allowed-packages", "example.jobs");
    return ApplicationContext.builder()
        .deduceEnvironment(false)
        .enableDefaultPropertySources(false)
        .singletons(singletons)
        .properties(values)
        .build();
  }

  static ApplicationContext engine(Map<String, Object> properties) {
    ApplicationContext context = core(properties);
    context.registerSingleton(JobStore.class, mock(JobStore.class));
    context.registerSingleton(RatchetRuntime.class, mock(RatchetRuntime.class));
    return context;
  }
}
