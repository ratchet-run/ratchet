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
package run.ratchet.testsuite.transaction;

import jakarta.enterprise.inject.spi.CDI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobContext;
import run.ratchet.api.TransactionalJob;
import run.ratchet.testsuite.app.TransactionalUnitProbe;

/** Jobs used only by the swallowed rollback integration tests. */
public class SwallowedRollbackJobs {
  public static final Map<String, Exception> FAILURES = new ConcurrentHashMap<>();

  public static final Map<String, AtomicInteger> ATTEMPTS = new ConcurrentHashMap<>();

  public static int attempts(String key) {
    AtomicInteger count = ATTEMPTS.get(key);
    return count == null ? 0 : count.get();
  }

  @TransactionalJob
  public static void body(String key) {
    ATTEMPTS.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
    writeAndSwallow(key);
  }

  @TransactionalJob
  public static void bodyOnce(String key) {
    int attempt = ATTEMPTS.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
    if (attempt == 1) {
      writeAndSwallow(key);
      return;
    }
    CDI.current()
        .select(TransactionalUnitProbe.class)
        .get()
        .insert(TransactionalUnitProbe.pageKey(key, 0));
  }

  public static void unit(String key) throws Exception {
    try {
      JobContext.current()
          .runInTransaction(
              after -> {
                writeAndSwallow(key);
                return "done";
              });
    } catch (Exception failure) {
      FAILURES.put(key, failure);
    }
  }

  private static void writeAndSwallow(String key) {
    CDI.current()
        .select(TransactionalUnitProbe.class)
        .get()
        .insert(TransactionalUnitProbe.pageKey(key, 0));
    try {
      CDI.current().select(NestedTransactionFailure.class).get().fail();
    } catch (RuntimeException expected) {
      // The caller continues while its transaction remains rollback-only.
    }
  }
}
