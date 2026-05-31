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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.annotation.PostConstruct;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

class NoOpClusterCoordinatorTest {

  @Test
  void announceMethodIsWiredViaPostConstruct() throws NoSuchMethodException {
    // The CDI container invokes @PostConstruct automatically; this test verifies the annotation
    // is in place so a future refactor that removes it gets caught here rather than only when
    // operators notice the missing startup log.
    Method announce = NoOpClusterCoordinator.class.getDeclaredMethod("announce");
    assertNotNull(
        announce.getAnnotation(PostConstruct.class), "@PostConstruct must remain on announce()");
  }

  @Test
  void announceDoesNotThrow() {
    NoOpClusterCoordinator coordinator = new NoOpClusterCoordinator();
    assertDoesNotThrow(
        () -> {
          // Invoke announce() reflectively (it's package-private and we're in the same package,
          // but call via getDeclaredMethod to keep coverage parity with @PostConstruct path).
          Method announce = NoOpClusterCoordinator.class.getDeclaredMethod("announce");
          announce.setAccessible(true);
          announce.invoke(coordinator);
        });
  }

  @Test
  void noOpMethodsAreSafe() {
    NoOpClusterCoordinator coordinator = new NoOpClusterCoordinator();
    assertDoesNotThrow(
        () -> coordinator.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null));
    assertDoesNotThrow(() -> coordinator.registerWakeupListener(hint -> {}));
    assertDoesNotThrow(coordinator::close);
  }
}
