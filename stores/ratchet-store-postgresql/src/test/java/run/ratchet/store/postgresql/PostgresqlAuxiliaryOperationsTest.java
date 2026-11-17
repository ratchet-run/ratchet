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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;

class PostgresqlAuxiliaryOperationsTest {

  @Test
  void cleanupProtectsRunningClaimForBothCallers() {
    for (boolean missing : List.of(false, true)) {
      EntityManager em = mock(EntityManager.class);
      Query query = mock(Query.class);
      when(query.setParameter(anyInt(), any())).thenReturn(query);
      when(query.executeUpdate()).thenReturn(2);
      when(em.createNativeQuery(any(String.class)))
          .thenAnswer(
              invocation -> {
                String sql = invocation.getArgument(0);
                assertTrue(sql.contains("DELETE FROM scheduler_resource_permit p"));
                assertTrue(sql.contains("NOT EXISTS ("));
                assertTrue(sql.contains("q.job_id = p.job_id"));
                assertTrue(sql.contains("q.status = 'RUNNING'"));
                assertTrue(sql.contains("q.picked_by = p.node_id"));
                assertTrue(sql.contains(missing ? "p.acquired_at < ?" : "p.node_id IN (?)"));
                if (missing) assertTrue(sql.contains("scheduler_node"));
                return query;
              });
      var operations = new PostgresqlAuxiliaryOperations(new PostgresqlStoreContext(em));
      assertEquals(
          2,
          missing
              ? operations.cleanupPermitsOfMissingNodes(Instant.EPOCH)
              : operations.cleanupOrphanedPermits(List.of("stale")));
      if (!missing) verify(query).setParameter(1, "stale");
      verify(query).executeUpdate();
    }
  }

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    PostgresqlAuxiliaryOperations operations =
        new PostgresqlAuxiliaryOperations(
            new PostgresqlStoreContext(entityManagerThrowingNoResult()));

    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS,
        operations.getPermitRetryDelay("missing-resource"));
  }

  private static EntityManager entityManagerThrowingNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException("missing");
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
}
