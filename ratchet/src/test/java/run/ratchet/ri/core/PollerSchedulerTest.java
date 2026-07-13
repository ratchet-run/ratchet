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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.internal.DefaultPollerScheduler;
import run.ratchet.ri.core.internal.PollerCycleExecutor;
import run.ratchet.spi.ExecutorProvider;

@ExtendWith(MockitoExtension.class)
class PollerSchedulerTest {

  @Mock private ExecutorProvider executorProvider;
  @Mock private PollerCycleExecutor pollerCycleExecutor;
  @Mock private ScheduledExecutorService executor;
  @Mock private ScheduledFuture<Object> handle;

  @Test
  void start_doesNotPublishStartedBeforeExecutorIsReady() {
    AtomicReference<DefaultPollerScheduler> schedulerRef = new AtomicReference<>();
    when(executorProvider.getScheduledExecutor())
        .thenAnswer(
            invocation -> {
              schedulerRef.get().wakeup();
              return executor;
            });
    doReturn(handle)
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    DefaultPollerScheduler scheduler =
        new DefaultPollerScheduler(executorProvider, pollerCycleExecutor);
    schedulerRef.set(scheduler);

    assertDoesNotThrow(scheduler::start);

    verify(executor).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }
}
