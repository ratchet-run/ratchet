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

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;
import run.ratchet.spi.JobInvocation;

/**
 * Invocation-mode streaming batch state: shares the chunk loop, chunk-boundary persistence, and
 * branch handling with {@link DefaultStreamingBatchBuilder} (which it extends) and carries a
 * per-item invocation factory instead of a serializable consumer. {@code DefaultJobCreationService}
 * dispatches each chunk to the invocation-mode child constructor when it sees this subtype; the
 * public surface is {@code DefaultInvocationStreamingBatchBuilder}.
 */
final class InvocationStreamingState<T extends Serializable>
    extends DefaultStreamingBatchBuilder<T> {

  private Function<T, JobInvocation> invocationFactory;

  InvocationStreamingState(String name, StreamingBatchSubmitter submitter) {
    super(name, submitter);
  }

  void setInvocationFactory(Function<T, JobInvocation> invocationFactory) {
    this.invocationFactory =
        Objects.requireNonNull(invocationFactory, "invocationFactory must not be null");
  }

  Function<T, JobInvocation> invocationFactory() {
    return invocationFactory;
  }

  @Override
  void validateReady() {
    if (stream() == null) {
      throw new IllegalStateException("Stream must be set via fromStream() before calling start()");
    }
    if (invocationFactory == null) {
      throw new IllegalStateException(
          "Invocation factory must be set via process() before calling start()");
    }
  }
}
