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
package run.ratchet.micronaut.internal;

import io.micronaut.data.connection.ConnectionStatus;
import io.micronaut.transaction.TransactionCallback;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionStatus;
import java.util.Optional;

/**
 * Fallback for applications without a transaction manager, including Mongo-only applications.
 * Transactions are inert, including MANDATORY, just as unproxied Spring engine beans are inert.
 * Never advertises an active transaction or a connection; this is not a SQL transaction manager. A
 * real TransactionOperations bean disables this factory's fallback.
 */
public final class NoTransactionOperations implements TransactionOperations<Object> {
  @Override
  public Object getConnection() {
    throw new IllegalStateException("No transaction manager configured");
  }

  @Override
  public boolean hasConnection() {
    return false;
  }

  @Override
  public Optional<? extends TransactionStatus<?>> findTransactionStatus() {
    return Optional.empty();
  }

  @Override
  public <R> R execute(TransactionDefinition definition, TransactionCallback<Object, R> callback) {
    var status = new InertStatus(definition);
    try {
      return callback.apply(status);
    } finally {
      status.completed = true;
    }
  }

  private static final class InertStatus implements TransactionStatus<Object> {
    private final TransactionDefinition definition;
    private boolean rollbackOnly;
    private boolean completed;

    InertStatus(TransactionDefinition definition) {
      this.definition = definition;
    }

    @Override
    public Object getTransaction() {
      return null;
    }

    @Override
    public ConnectionStatus<Object> getConnectionStatus() {
      throw new IllegalStateException("No transactional connection");
    }

    @Override
    public boolean isNewTransaction() {
      return false;
    }

    @Override
    public void setRollbackOnly() {
      rollbackOnly = true;
    }

    @Override
    public boolean isRollbackOnly() {
      return rollbackOnly;
    }

    @Override
    public boolean isCompleted() {
      return completed;
    }

    @Override
    public TransactionDefinition getTransactionDefinition() {
      return definition;
    }
  }
}
