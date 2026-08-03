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

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Internal helper that centralizes the identity-token bookkeeping shared by the reference
 * implementation's runtime-owned static holders ({@code EncryptionHolder}, {@code
 * PayloadSerializerHolder}, {@code EncryptionIntegrity}, {@code PayloadMaskingPolicyHolder}).
 *
 * <p>Ownership is compared by identity. A holder claims the guard for a token via {@link
 * #claim(Object, String)} (or the validating overload {@link #claim(Object, String, Supplier)});
 * re-claiming with the same token is idempotent. A different token is rejected with an {@link
 * IllegalStateException} carrying the caller's message, and the guard is left unchanged. {@link
 * #release(Object)} clears ownership only when the passed token is the current owner. {@link
 * #reset()} clears ownership unconditionally, for the anonymous install paths.
 *
 * <p>Not part of the public SPI.
 */
public final class OwnerTokenGuard {

  private Object ownerToken;

  /** Clears ownership unconditionally. Used by the anonymous (non-token) install paths. */
  public synchronized void reset() {
    ownerToken = null;
  }

  /**
   * Claims the guard for {@code ownerToken}.
   *
   * @throws IllegalStateException with {@code conflictMessage} if another token currently owns the
   *     guard
   */
  public synchronized void claim(Object ownerToken, String conflictMessage) {
    claim(ownerToken, conflictMessage, () -> null);
  }

  /**
   * Claims the guard for {@code ownerToken}, computing {@code valueSupplier} before the claim takes
   * effect.
   *
   * <p>Ownership is validated first; if another token owns the guard, {@code valueSupplier} is
   * never invoked. Otherwise {@code valueSupplier} runs and, only if it completes without throwing,
   * the token is claimed. A thrown exception leaves the guard's ownership untouched.
   *
   * @throws IllegalStateException with {@code conflictMessage} if another token currently owns the
   *     guard
   */
  public synchronized <T> T claim(
      Object ownerToken, String conflictMessage, Supplier<T> valueSupplier) {
    Objects.requireNonNull(ownerToken, "ownerToken");
    if (this.ownerToken != null && this.ownerToken != ownerToken) {
      throw new IllegalStateException(conflictMessage);
    }
    T value = valueSupplier.get();
    this.ownerToken = ownerToken;
    return value;
  }

  /**
   * Clears ownership only when {@code ownerToken} is the current owner.
   *
   * @return {@code true} if ownership was cleared, {@code false} if a different or no owner held
   *     the guard
   */
  public synchronized boolean release(Object ownerToken) {
    Objects.requireNonNull(ownerToken, "ownerToken");
    if (this.ownerToken == ownerToken) {
      this.ownerToken = null;
      return true;
    }
    return false;
  }
}
