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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;

class MysqlJobRowVersionTest {
  private static final UUID ID = UUID.fromString("01900000-0000-7000-8000-000000000001");
  private static final Instant SCHEDULED = Instant.parse("2026-11-14T00:00:00Z");

  @Test
  void staleRowVersionRejectsSaveBeforeAnyWrite() {
    var fixture = new SaveFixture(1, false);
    var job = job(2);
    job.setScheduledTime(SCHEDULED.plusSeconds(60));
    assertThrows(RatchetOptimisticLockException.class, () -> fixture.writes.save(job));
    assertEquals(0, fixture.updates.get());
    assertEquals(1, fixture.queries.get());
    assertEquals(2, job.getRowVersion());
  }

  @Test
  void staleTerminalRowVersionRejectsSaveBeforeAnyWrite() {
    var fixture = new SaveFixture(1, true);
    var job = job(2);
    job.setStatus(JobStatus.CANCELED);
    assertThrows(RatchetOptimisticLockException.class, () -> fixture.writes.save(job));
    assertEquals(0, fixture.updates.get());
    assertEquals(1, fixture.queries.get());
  }

  @Test
  void nullRowVersionRequiresStoreEntity() {
    var fixture = new SaveFixture(1, false);
    var failure =
        assertThrows(IllegalArgumentException.class, () -> fixture.writes.save(job(null)));
    assertEquals(
        "save() needs a job loaded from the store; rowVersion is null for job " + ID,
        failure.getMessage());
    assertEquals(0, fixture.updates.get());
    assertEquals(1, fixture.queries.get());
  }

  @Test
  void zeroRowsFromFinalUpdateRejectsSave() {
    var fixture = new SaveFixture(0, false);
    var job = job(3);
    assertThrows(RatchetOptimisticLockException.class, () -> fixture.writes.save(job));
    assertEquals(1, fixture.updates.get());
    assertEquals(3, job.getRowVersion());
  }

  @Test
  void successfulSaveAdvancesEntityRowVersionOnce() {
    var fixture = new SaveFixture(1, false);
    var job = job(3);
    assertSame(job, fixture.writes.save(job));
    assertEquals(4, job.getRowVersion());
    assertEquals(7, job.getVersion());
    assertEquals(1, fixture.updates.get());
  }

  private static JobEntity job(Integer rowVersion) {
    var job = new JobEntity();
    job.setId(ID);
    job.setStatus(JobStatus.PENDING);
    job.setVersion(7);
    job.setRowVersion(rowVersion);
    job.setScheduledTime(SCHEDULED);
    return job;
  }

  private static final class SaveFixture {
    final AtomicInteger updates = new AtomicInteger();
    final AtomicInteger queries = new AtomicInteger();
    final MysqlJobWriteOperations writes;

    SaveFixture(int updateCount, boolean terminal) {
      Object[] snapshot = {
        terminal ? null : "PENDING",
        SCHEDULED,
        0,
        null,
        null,
        null,
        null,
        7,
        terminal ? "CANCELED" : null,
        null,
        3
      };
      EntityManager em =
          (EntityManager)
              Proxy.newProxyInstance(
                  EntityManager.class.getClassLoader(),
                  new Class<?>[] {EntityManager.class},
                  (proxy, method, args) -> {
                    if (!method.getName().equals("createNativeQuery")) {
                      throw new UnsupportedOperationException(method.getName());
                    }
                    int statement = queries.getAndIncrement();
                    String sql = (String) args[0];
                    if (statement == 0) {
                      assertTrue(sql.contains("c.row_version"));
                    } else {
                      assertTrue(sql.contains("row_version = row_version + 1"));
                      assertTrue(sql.contains("AND row_version = ?"));
                    }
                    return Proxy.newProxyInstance(
                        Query.class.getClassLoader(),
                        new Class<?>[] {Query.class},
                        (query, queryMethod, parameters) -> {
                          return switch (queryMethod.getName()) {
                            case "setParameter" -> {
                              if (statement == 1 && Integer.valueOf(8).equals(parameters[0])) {
                                assertEquals(3, parameters[1]);
                              }
                              yield query;
                            }
                            case "getResultList" -> Collections.singletonList(snapshot);
                            case "executeUpdate" -> {
                              updates.incrementAndGet();
                              yield updateCount;
                            }
                            default ->
                                throw new UnsupportedOperationException(queryMethod.getName());
                          };
                        });
                  });
      var ctx = new MysqlStoreContext(em, null);
      writes =
          new MysqlJobWriteOperations(
              ctx, new MysqlBusinessKeyReservations(ctx), new MysqlTagOperations(ctx));
    }
  }
}
