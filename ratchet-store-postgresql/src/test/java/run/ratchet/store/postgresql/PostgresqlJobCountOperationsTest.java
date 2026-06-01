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
package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class PostgresqlJobCountOperationsTest {

  @Test
  void queueWaitPercentileRejectsOutOfRangeValueBeforeQuerying() {
    PostgresqlJobCountOperations operations =
        new PostgresqlJobCountOperations(new PostgresqlStoreContext(entityManagerThatMustNotRun()));

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> operations.getQueueWaitTimePercentile(1.5));

    assertTrue(thrown.getMessage().contains("[0.0, 1.0]"));
  }

  private static EntityManager entityManagerThatMustNotRun() {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              throw new AssertionError("EntityManager should not be called");
            });
  }
}
