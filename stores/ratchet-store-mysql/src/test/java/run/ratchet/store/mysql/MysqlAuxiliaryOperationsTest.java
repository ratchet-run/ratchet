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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

class MysqlAuxiliaryOperationsTest {

  @Test
  void tryAcquirePermit_sameJobExistingPermitIsIdempotent() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(entityManagerWithExistingPermit(jobId));

    assertTrue(operations.tryAcquirePermit("res", jobId, "node-1"));
  }

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(new MysqlStoreContext(entityManagerWithNoResult(), null));

    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS, operations.getPermitRetryDelay("missing"));
  }

  @Test
  void cleanupLocksAllResourcesInOrderBeforeDeletingAnyPermits() {
    EntityManager em = mock(EntityManager.class);
    Query discovery = mock(Query.class);
    Query lock = mock(Query.class);
    Query delete = mock(Query.class);
    when(em.createNativeQuery(any(String.class)))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0);
              if (sql.contains("SELECT DISTINCT")) return discovery;
              if (sql.contains("FOR UPDATE")) return lock;
              return delete;
            });
    for (Query query : List.of(discovery, lock, delete)) {
      when(query.setParameter(anyInt(), any())).thenReturn(query);
    }
    when(discovery.getResultList()).thenReturn(List.of("r-b", "r-a"));
    when(lock.getResultList()).thenReturn(List.of("resource"));
    when(delete.executeUpdate()).thenReturn(1, 2);
    var operations = new MysqlAuxiliaryOperations(new MysqlStoreContext(em, null));

    assertEquals(3, operations.cleanupOrphanedPermits(List.of("stale")));
    var order = inOrder(discovery, lock, delete);
    order.verify(discovery).getResultList();
    order.verify(lock).setParameter(1, "r-a");
    order.verify(lock).getResultList();
    order.verify(lock).setParameter(1, "r-b");
    order.verify(lock).getResultList();
    order.verify(delete).setParameter(1, "r-a");
    order.verify(delete).setParameter(2, "stale");
    order.verify(delete).executeUpdate();
    order.verify(delete).setParameter(1, "r-b");
    order.verify(delete).setParameter(2, "stale");
    order.verify(delete).executeUpdate();
  }

  private static EntityManager entityManagerWithNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException();
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return query;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static MysqlStoreContext entityManagerWithExistingPermit(UUID expectedJobId) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "createNativeQuery" ->
                        queryReturningExistingPermit(
                            expectedJobId, ((String) args[0]).contains("max_concurrent"));
                    case "persist" ->
                        throw new AssertionError("existing permit must not insert another row");
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return new MysqlStoreContext(em, null);
  }

  private static Query queryReturningExistingPermit(UUID expectedJobId, boolean limit) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "getResultList" ->
                    limit
                        ? List.of(2)
                        : Collections.singletonList(UuidByteArrayConverter.toBytes(expectedJobId));
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
