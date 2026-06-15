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
package run.ratchet.store.util;

import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.ConstraintDetector;

/**
 * Shared transient-fault translation for store operation classes.
 *
 * <p>Each store's {@code StoreContext} delegates here so the deadlock / transient-connection
 * classification lives in one place instead of being copy-pasted per dialect.
 */
public final class TransientStoreExceptions {

  private TransientStoreExceptions() {}

  /**
   * Wraps {@code e} as a {@link RatchetTransientStoreException} when {@code detector} classifies it
   * as a deadlock or a transient connection failure; otherwise returns {@code null} so the caller
   * can rethrow the original exception (or apply its own additional translation).
   *
   * @param dialectLabel human-readable store label used in the exception message (for example,
   *     {@code "MySQL"})
   * @param detector the store's constraint detector; implementations walk the cause chain
   * @param operation short description of the failing operation, for the message
   * @param e the runtime exception thrown by the JDBC / JPA layer
   * @return a wrapping {@link RatchetTransientStoreException}, or {@code null} when {@code e} is
   *     not a recognized transient fault
   */
  public static RatchetTransientStoreException translateOrNull(
      String dialectLabel, ConstraintDetector detector, String operation, RuntimeException e) {
    if (detector.isDeadlock(e) || detector.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient " + dialectLabel + " store concurrency failure during " + operation, e);
    }
    return null;
  }
}
