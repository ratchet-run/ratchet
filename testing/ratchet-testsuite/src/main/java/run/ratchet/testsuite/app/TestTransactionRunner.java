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
package run.ratchet.testsuite.app;

import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Runs test work inside its own {@code REQUIRES_NEW} transaction.
 *
 * <p>Wrapping request-thread store calls in this bean keeps the servlet frame safe from the
 * Payara/GlassFish {@code TransactionalInterceptorBase} TransactionOperationsManager race: no
 * background thread ever calls this bean, so its interceptor's save/restore of the frame's
 * transaction-operations state is reliable regardless of what the store bean's shared interceptor
 * does underneath. Tests that need an ambient transaction can use {@link
 * #runRollbackOnly(Runnable)} without driving {@link jakarta.transaction.UserTransaction} directly.
 */
@ApplicationScoped
@Transactional(Transactional.TxType.REQUIRES_NEW)
public class TestTransactionRunner {

  @Resource(lookup = "java:comp/TransactionSynchronizationRegistry")
  private TransactionSynchronizationRegistry transactionRegistry;

  public void run(Runnable work) {
    work.run();
  }

  /** Runs work in an interceptor-managed transaction and rolls it back when the method returns. */
  public void runRollbackOnly(Runnable work) {
    Object transactionKey =
        Objects.requireNonNull(
            transactionRegistry.getTransactionKey(), "transaction must be active");
    work.run();
    if (!transactionKey.equals(transactionRegistry.getTransactionKey())) {
      throw new IllegalStateException("caller transaction was not restored after work completed");
    }
    transactionRegistry.setRollbackOnly();
  }

  public <T> T call(Supplier<T> work) {
    return work.get();
  }
}
