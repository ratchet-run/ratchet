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

  @Override
  public void setJobUpdatedAt(long jobId, Instant updatedAt) {
    try {
      utx.begin();
      em.createNativeQuery("UPDATE scheduler_job SET updated_at = ?1 WHERE job_id = ?2")
          .setParameter(1, Timestamp.from(updatedAt))
          .setParameter(2, jobId)
          .executeUpdate();
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
