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

/** Framework-internal access to checkpoint bindings when restoring a context. */
public final class CheckpointBindings {
  private static volatile Function<JobContext, JobCheckpoints> reader;

  private CheckpointBindings() {}

  public static void installContextReader(Function<JobContext, JobCheckpoints> contextReader) {
    reader = Objects.requireNonNull(contextReader);
  }

  public static @Nullable JobCheckpoints checkpointsOf(@Nullable JobContext context) {
    return context == null ? null : reader.apply(context);
  }
}
