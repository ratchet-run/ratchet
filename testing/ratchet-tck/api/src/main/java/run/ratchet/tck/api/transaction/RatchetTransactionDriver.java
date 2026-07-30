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
package run.ratchet.tck.api.transaction;

import java.util.function.Supplier;

/**
 * Runtime-neutral transaction control used by the portable transaction contracts.
 *
 * <p>Implementations adapt the host transaction API without exposing it to {@code ratchet-tck-api}.
 */
public interface RatchetTransactionDriver {

  /** Begins a transaction, runs {@code work}, commits, and returns the work result. */
  <T> T committing(Supplier<T> work);

  /** Begins a transaction, runs {@code work}, and commits. */
  default void committing(Runnable work) {
    committing(
        () -> {
          work.run();
          return null;
        });
  }

  /** Begins a transaction, runs {@code work}, rolls back, and returns the work result. */
  <T> T rollingBack(Supplier<T> work);

  /** Begins a transaction, runs {@code work}, and rolls back. */
  default void rollingBack(Runnable work) {
    rollingBack(
        () -> {
          work.run();
          return null;
        });
  }
}
