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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobExecutionType;

class TransactionalPoolWarningTest {
  @Test
  void warnsOnceAtHeadroomBoundaryAcrossExecutions() {
    AtomicBoolean checked = new AtomicBoolean();
    List<String> messages = new ArrayList<>();
    PoolRegistry pools = pools(false);
    TransactionalPoolWarning.check(checked, () -> OptionalInt.of(9), pools, messages::add);
    TransactionalPoolWarning.check(checked, () -> OptionalInt.of(9), pools, messages::add);
    assertEquals(1, messages.size());
    assertTrue(messages.get(0).contains("7 configured platform workers"));
    assertTrue(messages.get(0).contains("connection pool of 9"));
    assertTrue(messages.get(0).contains("Raise the connection pool size or lower worker counts"));
    assertTrue(messages.get(0).contains("holds a connection for its whole body"));
  }

  @Test
  void largePoolIsSilentAndDoesNotRecheckLaterExecutions() {
    AtomicBoolean checked = new AtomicBoolean();
    List<String> messages = new ArrayList<>();
    TransactionalPoolWarning.check(checked, () -> OptionalInt.of(10), pools(false), messages::add);
    TransactionalPoolWarning.check(checked, () -> OptionalInt.of(2), pools(false), messages::add);
    assertTrue(messages.isEmpty());
  }

  @Test
  void unknownOrAbsentSizeIsSilent() {
    List<String> messages = new ArrayList<>();
    TransactionalPoolWarning.check(
        new AtomicBoolean(), OptionalInt::empty, pools(false), messages::add);
    TransactionalPoolWarning.check(new AtomicBoolean(), null, pools(false), messages::add);
    TransactionalPoolWarning.check(
        new AtomicBoolean(), OptionalInt::empty, pools(true), messages::add);
    assertTrue(messages.isEmpty());
  }

  @Test
  void virtualConcurrencyWarnsWithoutComparingWorkerCounts() {
    List<String> messages = new ArrayList<>();
    TransactionalPoolWarning.check(
        new AtomicBoolean(), () -> OptionalInt.of(10000), pools(true), messages::add);
    assertEquals(1, messages.size());
    assertTrue(
        messages
            .get(0)
            .contains("virtual-thread worker concurrency is unbounded relative to the pool"));
    assertTrue(messages.get(0).contains("10000"));
  }

  private PoolRegistry pools(boolean virtual) {
    Map<JobExecutionType, Integer> limits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) limits.put(type, 1);
    ThreadPoolManager platform =
        new ThreadPoolManager(
            ExecutorTargets.PLATFORM,
            mock(ExecutorProvider.class),
            null,
            ThreadPoolManager.AccountingMode.SEMAPHORE,
            limits);
    return new PoolRegistry(
        virtual
            ? Map.of(ExecutorTargets.PLATFORM, platform, ExecutorTargets.VIRTUAL, platform)
            : Map.of(ExecutorTargets.PLATFORM, platform));
  }
}
