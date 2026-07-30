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
package run.ratchet.tck.jakarta;

import jakarta.transaction.UserTransaction;
import java.util.Objects;
import java.util.function.Supplier;
import run.ratchet.tck.api.transaction.RatchetTransactionDriver;

final class UserTransactionRatchetTransactionDriver implements RatchetTransactionDriver {

  private final UserTransaction transaction;

  UserTransactionRatchetTransactionDriver(UserTransaction transaction) {
    this.transaction = Objects.requireNonNull(transaction, "transaction");
  }

  @Override
  public <T> T committing(Supplier<T> work) {
    return execute(work, true);
  }

  @Override
  public <T> T rollingBack(Supplier<T> work) {
    return execute(work, false);
  }

  private <T> T execute(Supplier<T> work, boolean commit) {
    Objects.requireNonNull(work, "work");
    boolean transactionActive = false;
    try {
      transaction.begin();
      transactionActive = true;
      T result = work.get();
      if (commit) {
        transaction.commit();
      } else {
        transaction.rollback();
      }
      transactionActive = false;
      return result;
    } catch (RuntimeException | Error e) {
      if (transactionActive) {
        rollbackAfterFailure(e);
      }
      throw e;
    } catch (Exception e) {
      if (transactionActive) {
        rollbackAfterFailure(e);
      }
      throw new IllegalStateException("Jakarta transaction control failed", e);
    }
  }

  private void rollbackAfterFailure(Throwable failure) {
    try {
      transaction.rollback();
    } catch (Exception rollbackFailure) {
      failure.addSuppressed(rollbackFailure);
    }
  }
}
