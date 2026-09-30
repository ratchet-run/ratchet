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

import run.ratchet.api.JobContext;

public final class CallbackRecorder {
  static JobContext context;
  static JobContext currentContext;
  static Throwable failure;

  private CallbackRecorder() {}

  static void reset() {
    context = null;
    currentContext = null;
    failure = null;
  }

  public static void success(JobContext ctx) {
    context = ctx;
    currentContext = JobContext.currentOrNull();
  }

  public static void failure(JobContext ctx, Throwable error) {
    success(ctx);
    failure = error;
  }
}
