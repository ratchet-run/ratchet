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
import java.util.function.BiPredicate;
import java.util.function.Function;
import run.ratchet.api.JobContext;
import run.ratchet.api.Nullable;
import run.ratchet.api.exception.CancellationRequestedException;

/**
 * Framework-internal bridge for an opaque object the runtime creates for each execution attempt.
 * Applications must not depend on this type.
 */
public final class AttemptTokens {
  private static volatile Function<JobContext, Object> contextReader;
  private static volatile BiPredicate<CancellationRequestedException, Object> stopMatcher;

  private AttemptTokens() {}

  public static void installContextReader(Function<JobContext, Object> reader) {
    contextReader = Objects.requireNonNull(reader);
  }

  public static void installStopMatcher(
      BiPredicate<CancellationRequestedException, Object> matcher) {
    stopMatcher = Objects.requireNonNull(matcher);
  }

  public static @Nullable Object tokenOf(@Nullable JobContext context) {
    return context == null ? null : contextReader.apply(context);
  }

  public static boolean isCooperativeStop(
      CancellationRequestedException exception, @Nullable Object attemptToken) {
    return stopMatcher.test(exception, attemptToken);
  }
}
