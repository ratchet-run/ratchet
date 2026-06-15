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

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobExecutionType;

class PostgresqlSignalOperationsTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000011");

  @Test
  void findTimedOutSignalJobsMapsOffsetDateTimeTimeouts() {
    OffsetDateTime timeout = OffsetDateTime.of(2026, 5, 12, 14, 30, 0, 0, ZoneOffset.UTC);
    var operations = new PostgresqlSignalOperations(contextReturning(row(timeout, 2)));

    var jobs = operations.findTimedOutSignalJobs(Instant.now(), 10);

    assertEquals(timeout.toInstant(), jobs.get(0).getSignalTimeout());
  }

  @Test
  void findTimedOutSignalJobsDefaultsInvalidPriorityOrdinal() {
    var operations =
        new PostgresqlSignalOperations(
            contextReturning(row(OffsetDateTime.now(ZoneOffset.UTC), 99)));

    var jobs = operations.findTimedOutSignalJobs(Instant.now(), 10);

    assertEquals(JobPriority.NORMAL, jobs.get(0).getPriority());
  }

  private static PostgresqlStoreContext contextReturning(Object[] row) {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter", "setMaxResults" -> proxy;
                    case "getResultList" -> Collections.singletonList(row);
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName())) {
                    return query;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    return new PostgresqlStoreContext(em);
  }

  private static Object[] row(OffsetDateTime timeout, int priorityOrdinal) {
    return new Object[] {
      JOB_ID,
      "approval",
      timeout,
      "WAITING",
      JobExecutionType.SINGLE.name(),
      priorityOrdinal,
      3,
      "business-key",
      BackoffPolicy.FIXED.name(),
      1000,
      "{}",
      "application/json",
      null,
      null,
      timeout,
      "tester",
      "delivery-1"
    };
  }
}
