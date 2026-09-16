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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TestEntityManagerProviderTest {
  @Test
  void equalTransactionKeysKeepLoadedEntitiesManaged() throws Exception {
    AtomicBoolean managed = new AtomicBoolean();
    AtomicInteger clears = new AtomicInteger();
    AtomicInteger transactionId = new AtomicInteger(1);
    Object entity = new Object();
    EntityManager delegate =
        proxy(
            EntityManager.class,
            (p, method, args) -> {
              return switch (method.getName()) {
                case "clear" -> {
                  managed.set(false);
                  clears.incrementAndGet();
                  yield null;
                }
                case "find" -> {
                  managed.set(true);
                  yield entity;
                }
                case "contains" -> managed.get();
                case "isOpen" -> true;
                case "hashCode" -> System.identityHashCode(p);
                case "equals" -> p == args[0];
                default -> null;
              };
            });
    EntityManagerFactory factory = proxy(EntityManagerFactory.class, (p, method, args) -> delegate);
    // The registry contract permits equal but distinct objects on successive calls.
    TransactionSynchronizationRegistry registry =
        proxy(
            TransactionSynchronizationRegistry.class,
            (p, method, args) -> new TransactionKey(transactionId.get()));
    TestEntityManagerProvider provider = new TestEntityManagerProvider();
    set(provider, "entityManagerFactory", factory);
    set(provider, "transactionSynchronizationRegistry", registry);

    EntityManager em = provider.getEntityManager();
    em.find(Object.class, 1);
    assertTrue(
        em.contains(entity), "A repeated key for the same transaction must not detach the entity");
    assertEquals(1, clears.get());
    transactionId.incrementAndGet();
    em.isOpen();
    assertEquals(2, clears.get());
  }

  private record TransactionKey(int id) {}

  private static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
  }

  private static void set(Object target, String name, Object value) throws Exception {
    var field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
