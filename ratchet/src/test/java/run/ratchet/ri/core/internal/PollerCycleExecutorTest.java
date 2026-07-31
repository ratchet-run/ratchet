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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PollerCycleExecutorTest {

  @Mock private Poller poller;

  @Test
  void transactionBoundarySuspendsInheritedContext() {
    Transactional transactional = PollerCycleExecutor.class.getAnnotation(Transactional.class);

    assertNotNull(transactional);
    assertEquals(TxType.NOT_SUPPORTED, transactional.value());
  }

  @Test
  void delegatesPollerCallbacks() {
    when(poller.tick()).thenReturn(37L);
    PollerCycleExecutor executor = new PollerCycleExecutor(poller);

    assertEquals(37L, executor.tick());
    executor.onWakeup();

    verify(poller).tick();
    verify(poller).onWakeup();
  }

  @Test
  void portableSupplierResolvesThePollerLazily() {
    AtomicInteger resolutions = new AtomicInteger();
    when(poller.tick()).thenReturn(11L);
    PollerCycleExecutor executor =
        new PollerCycleExecutor(
            () -> {
              resolutions.incrementAndGet();
              return poller;
            });

    assertEquals(0, resolutions.get());
    assertEquals(11L, executor.tick());
    executor.onWakeup();

    assertEquals(2, resolutions.get());
  }
}
