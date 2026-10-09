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

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import run.ratchet.store.converter.RuntimeContextInstallation;

class MicronautRuntimeOwnershipTest {
  @Test
  void failedStartOnSameThreadIsReclaimed() {
    var probe = new FailureProbe();
    try (var failed =
        TestContexts.core(Map.of("ratchet.test.fail-after-installation", true), probe)) {
      try {
        Throwable failure = assertThrows(RuntimeException.class, failed::start);
        assertEquals("Failure after runtime installation", rootCause(failure).getMessage());
        assertNotNull(probe.installation);
        assertFalse(failed.isRunning());
        assertSuccessfulReplacement(probe.installation);
      } finally {
        probe.close();
      }
    }
  }

  @Test
  void runningContextIsNeverReclaimed() {
    try (var first = TestContexts.core(Map.of());
        var second = TestContexts.core(Map.of())) {
      first.start();
      var installation = first.getBean(RuntimeContextInstallation.class);
      Throwable failure = assertThrows(RuntimeException.class, second::start);
      assertTrue(rootCause(failure).getMessage().contains("Another Ratchet runtime already owns"));
      assertTrue(first.isRunning());
      assertSame(installation, first.getBean(RuntimeContextInstallation.class));
    }
  }

  @Test
  void failedStartOnTerminatedThreadIsReclaimed() throws InterruptedException {
    var probe = new FailureProbe();
    var failure = new AtomicReference<Throwable>();
    try (var failed =
        TestContexts.core(Map.of("ratchet.test.fail-after-installation", true), probe)) {
      Thread starter =
          new Thread(
              () -> {
                try {
                  failed.start();
                } catch (Throwable thrown) {
                  failure.set(thrown);
                }
              });
      try {
        starter.start();
        starter.join();
        assertFalse(starter.isAlive());
        assertNotNull(failure.get());
        assertEquals("Failure after runtime installation", rootCause(failure.get()).getMessage());
        assertNotNull(probe.installation);
        assertFalse(failed.isRunning());
        assertSuccessfulReplacement(probe.installation);
      } finally {
        starter.join();
        probe.close();
      }
    }
  }

  private static void assertSuccessfulReplacement(RuntimeContextInstallation abandoned) {
    try (var replacement = TestContexts.core(Map.of())) {
      replacement.start();
      assertTrue(replacement.isRunning());
      assertNotSame(abandoned, replacement.getBean(RuntimeContextInstallation.class));
    }
  }

  private static Throwable rootCause(Throwable failure) {
    while (failure.getCause() != null) failure = failure.getCause();
    return failure;
  }

  static final class FailureProbe implements AutoCloseable {
    volatile RuntimeContextInstallation installation;

    @Override
    public void close() {
      if (installation != null) installation.close();
    }
  }

  @Context
  @Requires(property = "ratchet.test.fail-after-installation", value = "true")
  static class FailingStartupBean {
    FailingStartupBean(RuntimeContextInstallation installation, FailureProbe probe) {
      probe.installation = installation;
      throw new IllegalStateException("Failure after runtime installation");
    }
  }
}
