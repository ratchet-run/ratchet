package run.ratchet.testsuite.app;

import run.ratchet.store.spi.RatchetEntityManagerProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
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

  @Inject private RatchetEntityManagerProvider entityManagerProvider;

  @Inject private UserTransaction utx;

  @Override
  public void setJobUpdatedAt(long jobId, Instant updatedAt) {
    try {
      utx.begin();
      Timestamp ts = Timestamp.from(updatedAt);

      // Both JPA stores are now hot/cold-split: cold scheduler_job has no updated_at; the
      // archive/DLQ-purge cutoff lives on cold.terminated_at, and the live update timestamp
      // lives on scheduler_job_queue.updated_at. Tests aim this method at one or the other
      // depending on the row's lifecycle stage.
      // language=SQL
      String coldSql =
          """
          UPDATE scheduler_job SET terminated_at = ?1
          WHERE job_id = ?2 AND terminal_status IS NOT NULL
          """;
      em().createNativeQuery(coldSql).setParameter(1, ts).setParameter(2, jobId).executeUpdate();
      try {
        // language=SQL
        String hotSql = "UPDATE scheduler_job_queue SET updated_at = ?1 WHERE job_id = ?2";
        em().createNativeQuery(hotSql).setParameter(1, ts).setParameter(2, jobId).executeUpdate();
      } catch (RuntimeException ignored) {
        // The queue row may not exist once a job has moved to the terminal table.
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

  private EntityManager em() {
    return entityManagerProvider.getEntityManager();
  }
}
