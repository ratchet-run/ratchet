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
package run.ratchet.spring.boot.it.compatibility;

import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.tck.api.transaction.RatchetTransactionDriver;

final class SpringTransactionDriver implements RatchetTransactionDriver {

  private final TransactionTemplate transactionTemplate;

  SpringTransactionDriver(TransactionTemplate transactionTemplate) {
    this.transactionTemplate =
        Objects.requireNonNull(transactionTemplate, "transactionTemplate must not be null");
  }

  @Override
  public <T> T committing(Supplier<T> work) {
    return transactionTemplate.execute(status -> work.get());
  }

  @Override
  public <T> T rollingBack(Supplier<T> work) {
    return transactionTemplate.execute(
        status -> {
          T result = work.get();
          status.setRollbackOnly();
          return result;
        });
  }
}
