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
package run.ratchet.consumer.sql;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionStatus;
import jakarta.persistence.EntityManager;
import java.util.function.Consumer;

/** Native queries join the same host transaction as application entities and Ratchet. */
public final class SqlAccess {
  private final EntityManager entityManager;
  private final TransactionOperations<EntityManager> transactions;

  @SuppressWarnings("unchecked")
  public SqlAccess(ApplicationContext context) {
    entityManager = context.getBean(EntityManager.class);
    transactions =
        (TransactionOperations<EntityManager>)
            context.getBean(
                Argument.of(TransactionOperations.class, EntityManager.class),
                Qualifiers.byName("default"));
  }

  public void transaction(Consumer<TransactionStatus<EntityManager>> action) {
    transactions.executeWrite(
        status -> {
          action.accept(status);
          return null;
        });
  }

  public void update(String sql, Object... arguments) {
    transactions.executeWrite(
        status -> {
          var query = entityManager.createNativeQuery(sql);
          for (int i = 0; i < arguments.length; i++) query.setParameter(i + 1, arguments[i]);
          query.executeUpdate();
          return null;
        });
  }

  public void execute(String sql) {
    update(sql);
  }

  public <T> T queryForObject(String sql, Class<T> type, Object... arguments) {
    return transactions.executeRead(
        status -> {
          var query = entityManager.createNativeQuery(sql);
          for (int i = 0; i < arguments.length; i++) query.setParameter(i + 1, arguments[i]);
          Object result = query.getSingleResult();
          if (type == Integer.class && result instanceof Number number)
            return type.cast(number.intValue());
          return type.cast(result);
        });
  }
}
