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
package example.ratchet;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.transaction.hibernate.HibernateTransactionManager;
import io.micronaut.transaction.jdbc.DataSourceTransactionManager;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobContext;

/** Application jobs enabled only by the checkpoint consumer tests. */
@Singleton
@Requires(property = "consumer.checkpoint-units", value = "true")
public class CheckpointUnitJobs {
  private final BeanContext beans;
  private final AtomicInteger attempts = new AtomicInteger();
  public final CountDownLatch inside = new CountDownLatch(1);
  public final CountDownLatch release = new CountDownLatch(1);
  public final List<Optional<String>> received = new CopyOnWriteArrayList<>();
  public volatile Exception caught;
  public volatile boolean unitRan;
  public volatile boolean otherTransactionActive;
  public volatile String retryFirst;

  public CheckpointUnitJobs(BeanContext beans) {
    this.beans = beans;
  }

  public void execute(String prefix, String scenario) throws Exception {
    JobContext ctx = JobContext.current();
    int attempt = attempts.incrementAndGet();
    if (scenario.equals("other")) {
      var other = beans.getBean(DataSourceTransactionManager.class, Qualifiers.byName("other"));
      other.executeWrite(
          status -> {
            otherTransactionActive = other.findTransactionStatus().isPresent();
            try {
              ctx.runInTransaction(
                  cp -> {
                    unitRan = true;
                    insert(prefix + "-0");
                    return "done";
                  });
            } catch (Exception failure) {
              caught = failure;
            }
            return null;
          });
      return;
    }
    if (scenario.equals("rollbackOnly")) {
      try {
        ctx.runInTransaction(
            cp -> {
              insert(prefix + "-0");
              beans
                  .getBean(HibernateTransactionManager.class, Qualifiers.byName("default"))
                  .findTransactionStatus()
                  .orElseThrow()
                  .setRollbackOnly();
              return "done";
            });
      } catch (Exception failure) {
        caught = failure;
      }
      return;
    }
    boolean first = true;
    while (true) {
      boolean firstUnit = first;
      String next =
          ctx.runInTransaction(
              cp -> {
                received.add(Optional.ofNullable(cp));
                if (attempt == 2 && firstUnit) retryFirst = cp;
                int page = cp == null ? 0 : Integer.parseInt(cp);
                insert(prefix + "-" + page);
                if (scenario.equals("atomic")) {
                  inside.countDown();
                  if (!release.await(30, TimeUnit.SECONDS))
                    throw new IllegalStateException("Unit not released");
                  return "done";
                }
                if (attempt == 1 && page == 1) throw new IOException("Failed second unit");
                return page == 2 ? "done" : Integer.toString(page + 1);
              });
      first = false;
      if (next.equals("done")) return;
    }
  }

  private void insert(String id) {
    beans
        .getBean(HibernateTransactionManager.class, Qualifiers.byName("default"))
        .getConnection()
        .createNativeQuery("INSERT INTO consumer_record (id, state) VALUES (?1, ?2)")
        .setParameter(1, id)
        .setParameter(2, "unit")
        .executeUpdate();
  }
}
