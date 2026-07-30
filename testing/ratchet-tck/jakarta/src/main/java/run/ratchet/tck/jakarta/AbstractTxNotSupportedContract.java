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

import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import run.ratchet.tck.api.transaction.RatchetTransactionDriver;

/** Jakarta {@link UserTransaction} adapter for the portable not-supported transaction contract. */
public abstract class AbstractTxNotSupportedContract
    extends run.ratchet.tck.api.transaction.AbstractTxNotSupportedContract {

  @Inject protected UserTransaction tx;

  @Override
  protected RatchetTransactionDriver transactionDriver() {
    return new UserTransactionRatchetTransactionDriver(tx);
  }
}
