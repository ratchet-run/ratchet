package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.UserTransaction;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * JPA/SQL implementation of {@link TestDataManipulator}.
 *
 * <p>Uses native SQL queries within JTA transactions to manipulate test data. Only packaged in the
 * WAR when a JPA store profile is active.
 */
@ApplicationScoped
public class JpaTestDataManipulator implements TestDataManipulator {

  @PersistenceContext private EntityManager em;

  @Inject private UserTransaction utx;

  private static boolean isPostgresql() {
    return "postgresql".equalsIgnoreCase(System.getProperty("ratchet.test.db.type", "mysql"));
  }

  @Override
  public void setJobUpdatedAt(long jobId, Instant updatedAt) {
    try {
      utx.begin();
      Timestamp ts = Timestamp.from(updatedAt);

      if (isPostgresql()) {
        em.createNativeQuery("UPDATE scheduler_job SET updated_at = ?1 WHERE job_id = ?2")
            .setParameter(1, ts)
            .setParameter(2, jobId)
            .executeUpdate();
      } else {
        // Post hot/cold-split: cold has no updated_at column. Tests using this method aim the
        // time at archiving/DLQ-purge cutoffs (cold.terminated_at) or live update timestamps
        // (hot.updated_at).
        em.createNativeQuery(
                "UPDATE scheduler_job SET terminated_at = ?1 "
                    + "WHERE job_id = ?2 AND terminal_status IS NOT NULL")
            .setParameter(1, ts)
            .setParameter(2, jobId)
            .executeUpdate();
        try {
          em.createNativeQuery("UPDATE scheduler_job_queue SET updated_at = ?1 WHERE job_id = ?2")
              .setParameter(1, ts)
              .setParameter(2, jobId)
              .executeUpdate();
        } catch (RuntimeException ignored) {
          // The queue row may not exist once a job has moved to the terminal table.
        }
      }

      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly();
      throw e;
    } catch (Exception e) {
      rollbackQuietly();
      throw new RuntimeException("Failed to set updated_at", e);
    }
  }

  private void rollbackQuietly() {
    try {
      utx.rollback();
    } catch (Exception ignored) {
      // best-effort rollback
    }
  }
}
