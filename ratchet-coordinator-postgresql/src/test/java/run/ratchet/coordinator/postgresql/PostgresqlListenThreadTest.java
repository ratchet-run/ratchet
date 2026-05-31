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
package run.ratchet.coordinator.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.postgresql.PGNotification;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

class PostgresqlListenThreadTest {

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();

  @Test
  void oversizedPayloadIncrementsParseFailureBeforeDecode() {
    AtomicInteger dispatched = new AtomicInteger();
    AtomicInteger parseFailures = new AtomicInteger();
    PostgresqlCoordinatorConfig config =
        new PostgresqlCoordinatorConfig(
            "ratchet_wakeup", Optional.empty(), 100L, 10L, 50L, 4, 1, 500L);
    PostgresqlListenThread listener =
        new PostgresqlListenThread(
            new PostgresqlConnectionLifecycle(
                () -> {
                  throw new AssertionError("connection should not be acquired");
                },
                config,
                ms -> {}),
            codec,
            config,
            p -> dispatched.incrementAndGet(),
            parseFailures::incrementAndGet,
            () -> {});
    PGNotification notification = mock(PGNotification.class);
    when(notification.getParameter()).thenReturn("{\"v\":1,\"node\":\"nodeA\",\"prio\":\"HIGH\"}");

    listener.dispatchOne(notification);

    assertEquals(0, dispatched.get());
    assertEquals(1, parseFailures.get());
  }
}
