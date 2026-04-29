package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import jakarta.persistence.Query;
import java.util.Collections;
import java.util.List;

final class PostgresqlBusinessKeyReservations {

  static final String OWNER_TABLE_QUEUE = "QUEUE";
  static final String OWNER_TABLE_RECURRING = "RECURRING";

  private final PostgresqlStoreContext ctx;

  PostgresqlBusinessKeyReservations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  static String ownerTableFor(JobExecutionType jobType) {
    return jobType == JobExecutionType.RECURRING ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
  }

  static String ownerTableFor(String jobType) {
    return "RECURRING".equals(jobType) ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
  }

  void insertReservation(String businessKey, long ownerJobId, String ownerTable) {
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_business_key_reservation
          (business_key, owner_job_id, owner_table, reserved_at)
        VALUES (?, ?, ?, statement_timestamp())
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, businessKey)
        .setParameter(2, ownerJobId)
        .setParameter(3, ownerTable)
        .executeUpdate();
  }

  @SuppressWarnings("UnusedReturnValue")
  int deleteReservationByOwner(long ownerJobId) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id = ?";
    return ctx.em().createNativeQuery(sql).setParameter(1, ownerJobId).executeUpdate();
  }

  void deleteReservationsByOwners(List<? extends Number> ownerJobIds) {
    if (ownerJobIds.isEmpty()) {
      return;
    }
    String placeholders = String.join(",", Collections.nCopies(ownerJobIds.size(), "?"));
    // language=PostgreSQL
    String sql =
        "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
            + placeholders
            + ")";
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (Number ownerJobId : ownerJobIds) {
      query.setParameter(parameter++, ownerJobId.longValue());
    }
    query.executeUpdate();
  }

  void syncForJob(JobEntity job) {
    deleteReservationByOwner(job.getId());
    JobStatus status = PostgresqlStoreContext.effectiveStatus(job.getStatus());
    if (PostgresqlStoreContext.isLiveStatus(status) && job.getBusinessKey() != null) {
      insertReservation(job.getBusinessKey(), job.getId(), ownerTableFor(job.getJobType()));
    }
  }

  void syncForJob(long ownerJobId, JobStatus status) {
    deleteReservationByOwner(ownerJobId);
    if (!PostgresqlStoreContext.isLiveStatus(status)) {
      return;
    }
    // language=PostgreSQL
    String selectSql = "SELECT business_key, job_type FROM scheduler_job WHERE job_id = ?";
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        ctx.em().createNativeQuery(selectSql).setParameter(1, ownerJobId).getResultList();
    if (rows.isEmpty()) {
      return;
    }
    Object[] row = rows.get(0);
    String businessKey = (String) row[0];
    if (businessKey != null) {
      insertReservation(businessKey, ownerJobId, ownerTableFor((String) row[1]));
    }
  }
}
