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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import run.ratchet.api.BatchContext;

/** Tracks batch progress callbacks and completion status for integration tests. */
public class BatchCompletionTracker {

  private static final CopyOnWriteArrayList<BatchContext> PROGRESS_SNAPSHOTS =
      new CopyOnWriteArrayList<>();
  private static final AtomicBoolean BATCH_COMPLETE = new AtomicBoolean(false);

  public static void onProgress(BatchContext context) {
    PROGRESS_SNAPSHOTS.add(context);
    if (context.isComplete()) {
      BATCH_COMPLETE.set(true);
    }
  }

  public static List<BatchContext> progressSnapshots() {
    return List.copyOf(PROGRESS_SNAPSHOTS);
  }

  public static boolean isBatchComplete() {
    return BATCH_COMPLETE.get();
  }

  public static void reset() {
    PROGRESS_SNAPSHOTS.clear();
    BATCH_COMPLETE.set(false);
  }
}
