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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutionTuningProvider;

class DefaultExecutionTuningProviderTest {

  @Test
  void supportsSchedulerInternalExecutionTypeNames() {
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(
                execution ->
                    execution
                        .maxConcurrency("WORKFLOW_BRANCH", 4)
                        .virtualThreadLimit("WORKFLOW_JOIN", 19))
            .build();

    ExecutionTuningProvider provider = new DefaultExecutionTuningProvider(options);

    assertEquals(4, provider.maxConcurrency("WORKFLOW_BRANCH", 2));
    assertEquals(19, provider.virtualThreadLimit("WORKFLOW_JOIN", 1000));
  }

  @Test
  void defaultsThreadingModeToPlatform() {
    ExecutionTuningProvider provider =
        new DefaultExecutionTuningProvider(RatchetOptions.defaults());

    assertEquals(RatchetOptions.ThreadingMode.PLATFORM, provider.defaultThreadingMode());
  }

  @Test
  void protectedConstructorFailsClearlyWhenUsedWithoutInjection() {
    ExecutionTuningProvider provider = new DefaultExecutionTuningProvider();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, provider::defaultThreadingMode);

    assertEquals("RatchetOptions were not injected", thrown.getMessage());
  }
}
