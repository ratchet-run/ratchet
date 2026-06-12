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

import java.io.Serial;
import java.util.Objects;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.spi.JobInvocation;

/**
 * Carrier that lets a pre-resolved {@link JobInvocation} ride through builder slots typed {@link
 * SerializableCheckedRunnable} (task, chain steps, branch targets). The creation path unwraps it
 * before lambda resolution; it is never executed and never serialized.
 */
final class InvocationAdapter implements SerializableCheckedRunnable {

  @Serial private static final long serialVersionUID = 1L;

  private final JobInvocation invocation;

  InvocationAdapter(JobInvocation invocation) {
    this.invocation = Objects.requireNonNull(invocation, "invocation must not be null");
  }

  JobInvocation invocation() {
    return invocation;
  }

  @Override
  public void run() {
    throw new IllegalStateException(
        "InvocationAdapter is a persistence-time carrier and must never be executed");
  }
}
