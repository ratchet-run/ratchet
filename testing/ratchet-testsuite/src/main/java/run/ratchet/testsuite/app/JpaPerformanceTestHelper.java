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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.List;
import java.util.logging.Logger;
import run.ratchet.store.spi.RatchetEntityManagerProvider;
import run.ratchet.tck.store.SqlDialectTestSupport;

/** JPA implementation using native bulk inserts. */
@ApplicationScoped
public class JpaPerformanceTestHelper implements PerformanceTestHelper {

  private static final Logger log = Logger.getLogger(JpaPerformanceTestHelper.class.getName());

  @Inject private RatchetEntityManagerProvider entityManagerProvider;

  @Inject private UserTransaction utx;

  @Override
  public void insertBackgroundRows(int count, String keyPrefix) {
    SqlDialectTestSupport dialect = SqlDialectTestSupportProvider.get();
    int chunkSize = 100_000;

    try {
      for (int offset = 0; offset < count; offset += chunkSize) {
        int batchCount = Math.min(chunkSize, count - offset);

        utx.begin();
        dialect.insertBackgroundChunk(em(), batchCount, offset, keyPrefix);
        utx.commit();

        if (count > chunkSize) {
          log.info(
              String.format("  ... inserted %d / %d background rows", offset + batchCount, count));
        }
      }

      // Refresh table statistics for accurate query planner estimates
      utx.begin();
      dialect.analyzeSchedulerJob(em());
      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly();
      throw e;
    } catch (Exception e) {
      rollbackQuietly();
      throw new RuntimeException("Bulk insert error", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public long queryQueueWaitPercentileForClass(String targetClass, double percentile) {
    try {
      utx.begin();
      // language=SQL
      String sql =
          """
          SELECT queue_wait_ms FROM scheduler_job
          WHERE target_class = :cls AND status = 'SUCCEEDED'
            AND queue_wait_ms IS NOT NULL
          ORDER BY queue_wait_ms
          """;
      List<Number> results =
          em().createNativeQuery(sql).setParameter("cls", targetClass).getResultList();
      utx.commit();

      if (results.isEmpty()) {
        return 0;
      }
      int index = (int) Math.ceil(percentile * results.size()) - 1;
      return results.get(Math.max(0, index)).longValue();
    } catch (Exception e) {
      rollbackQuietly();
      log.warning("queue_wait_ms query error: " + e.getMessage());
      return -1;
    }
  }

  @Override
  public void assertNoFullScan(String label, Runnable storeOperation) {
    try {
      SqlDialectTestSupportProvider.get().assertNoFullScan(em(), utx, label, storeOperation);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Scan check error", e);
    }
  }

  private EntityManager em() {
    return entityManagerProvider.getEntityManager();
  }

  private void rollbackQuietly() {
    try {
      utx.rollback();
    } catch (Exception ignored) {
      // best-effort rollback
    }
  }
}
