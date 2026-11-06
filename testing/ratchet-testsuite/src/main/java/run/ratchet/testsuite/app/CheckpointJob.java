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
package run.ratchet.testsuite.app;

import jakarta.enterprise.inject.spi.CDI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import run.ratchet.api.JobContext;
import run.ratchet.store.spi.JobStore;

/** Records checkpoint observations from real scheduler attempts. */
public class CheckpointJob {
  public static final Map<UUID, List<Optional<String>>> OBSERVED = new ConcurrentHashMap<>();
  public static volatile CountDownLatch written = new CountDownLatch(1);
  public static volatile CountDownLatch release = new CountDownLatch(1);

  public static void retryCommitted() {
    retry(false);
  }

  public static void retryRolledBack() {
    retry(true);
  }

  private static void retry(boolean rollback) {
    JobContext ctx = JobContext.current();
    var observations = OBSERVED.computeIfAbsent(ctx.jobId(), id -> new CopyOnWriteArrayList<>());
    if (observations.isEmpty()) {
      var tx = CDI.current().select(CheckpointTransactions.class).get();
      tx.run(() -> ctx.checkpoint("page-1"));
      if (rollback) {
        try {
          tx.rollback(() -> ctx.checkpoint("page-2"));
        } catch (IllegalStateException expected) {
          if (!"Roll back checkpoint transaction".equals(expected.getMessage())) throw expected;
        }
      }
      observations.add(ctx.lastCheckpoint());
      throw new IllegalStateException("Retry after checkpoint");
    }
    observations.add(ctx.lastCheckpoint());
  }

  public static void immediate() throws InterruptedException {
    JobContext.current().checkpoint("now");
    written.countDown();
    if (!release.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Job not released");
  }

  public static volatile CountDownLatch pressureStart = new CountDownLatch(1);

  public static void pressure(String value) throws InterruptedException {
    if (!pressureStart.await(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Pressure burst was not released");
    }
    JobContext ctx = JobContext.current();
    CDI.current()
        .select(CheckpointTransactions.class)
        .get()
        .run(
            () -> {
              // Enlist the datasource before commit. An empty transaction would not exercise the
              // second-connection hazard of store I/O from an after-commit callback.
              CDI.current().select(JobStore.class).get().findById(ctx.jobId()).orElseThrow();
              ctx.checkpoint(value);
              try {
                Thread.sleep(25);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Pressure job interrupted", interrupted);
              }
            });
  }

  public static void reset() {
    release.countDown();
    OBSERVED.clear();
    written = new CountDownLatch(1);
    release = new CountDownLatch(1);
  }
}
