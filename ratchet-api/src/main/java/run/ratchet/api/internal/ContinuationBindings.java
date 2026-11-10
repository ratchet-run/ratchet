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
package run.ratchet.api.internal;

import java.util.Objects;
import java.util.function.Function;
import run.ratchet.api.JobContext;
import run.ratchet.api.Nullable;

/** Framework-internal access to continuation bindings when restoring a context. */
public final class ContinuationBindings {
  private static volatile Function<JobContext, JobContinuation> reader;

  private ContinuationBindings() {}

  /**
   * Installs the framework reader for a context's continuation request.
   *
   * @param contextReader reader installed by {@link JobContext}
   * @throws NullPointerException if the reader is null
   */
  public static void installContextReader(Function<JobContext, JobContinuation> contextReader) {
    reader = Objects.requireNonNull(contextReader);
  }

  /**
   * Returns the same continuation request instance that {@link JobContext#requestContinuation()}
   * changes.
   *
   * @param context context to inspect, or null
   * @return the continuation request, or null for a null context or a context without a persisted
   *     attempt or checkpoint bindings
   */
  public static @Nullable JobContinuation continuationOf(@Nullable JobContext context) {
    return context == null ? null : reader.apply(context);
  }
}
