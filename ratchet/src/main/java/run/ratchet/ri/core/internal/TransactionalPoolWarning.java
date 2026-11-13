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

import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.ExecutorTargets;

/** Checks connection headroom on the process's first transactional execution. */
final class TransactionalPoolWarning {
  private static final Logger LOG = Logger.getLogger(TransactionalPoolWarning.class);
  private static final AtomicBoolean CHECKED = new AtomicBoolean();

  private TransactionalPoolWarning() {}

  static void check(ConnectionPoolSize size, PoolRegistry pools) {
    check(CHECKED, size, pools, LOG::warn);
  }

  static void check(
      AtomicBoolean checked, ConnectionPoolSize size, PoolRegistry pools, Consumer<String> warn) {
    if (!checked.compareAndSet(false, true) || size == null || pools == null) return;
    OptionalInt maximum = size.maximumPoolSize();
    if (maximum.isEmpty()) return;
    int poolSize = maximum.getAsInt();
    String fix =
        " Raise the connection pool size or lower worker counts; each running transactional job"
            + " holds a connection for its whole body.";
    if (pools.hasPool(ExecutorTargets.VIRTUAL)) {
      warn.accept(
          "Transactional jobs use a connection pool of "
              + poolSize
              + "; virtual-thread worker concurrency is unbounded relative to the pool."
              + fix);
      return;
    }
    int workers =
        pools.pool(ExecutorTargets.PLATFORM).getThreadPoolHealth().values().stream()
            .mapToInt(ThreadPoolManager.ThreadPoolHealth::maxPoolSize)
            .sum();
    if (workers >= poolSize - 2) {
      warn.accept(
          "Transactional jobs have "
              + workers
              + " configured platform workers and a connection pool of "
              + poolSize
              + "."
              + fix);
    }
  }
}
