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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.RatchetRuntime;

class MicronautRatchetLifecycleTest {
  static class Persistence {
    final RatchetRuntime runtime;

    Persistence(RatchetRuntime runtime) {
      this.runtime = runtime;
    }

    @PreDestroy
    void close() {
      verify(runtime).close();
    }
  }

  @Factory
  @Requires(property = "test.shutdown", value = "true")
  static class PersistenceFactory {
    @Singleton
    Persistence persistence(RatchetRuntime runtime) {
      return new Persistence(runtime);
    }
  }

  @Test
  void shutdownDrainsBeforePersistenceDestructionAndClosesOnce() {
    RatchetRuntime runtime;
    try (var context = TestContexts.engine(Map.of("test.shutdown", true))) {
      context.start();
      runtime = context.getBean(RatchetRuntime.class);
      context.getBean(Persistence.class);
      verify(runtime).start();
      assertTrue(context.getBean(MicronautRatchetLifecycle.class).isRunning());
    }
    verify(runtime, times(1)).close();
  }
}
