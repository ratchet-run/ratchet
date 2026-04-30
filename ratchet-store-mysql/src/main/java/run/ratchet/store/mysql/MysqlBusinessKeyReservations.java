package run.ratchet.store.mysql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.util.UUID;

final class MysqlBusinessKeyReservations {

  static final String OWNER_TABLE_QUEUE = "QUEUE";
  static final String OWNER_TABLE_RECURRING = "RECURRING";

  // language=MySQL
  static final String BKRES_INSERT_SQL =
      """
      INSERT INTO scheduler_business_key_reservation
        (business_key, owner_job_id, owner_table, reserved_at)
      VALUES (?, ?, ?, ?)
      """;

  private final MysqlStoreContext ctx;

  MysqlBusinessKeyReservations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  void insertReservation(String businessKey, UUID ownerJobId, String ownerTable) {
    if (businessKey == null) {
      return;
    }
    // language=MySQL
    String sql =
        """
        INSERT INTO scheduler_business_key_reservation
          (business_key, owner_job_id, owner_table, reserved_at)
        VALUES (?, ?, ?, NOW(3))
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, businessKey)
        .setParameter(2, ownerJobId)
        .setParameter(3, ownerTable)
        .executeUpdate();
  }

  @SuppressWarnings("UnusedReturnValue")
  int deleteReservationByOwner(UUID ownerJobId) {
    // language=MySQL
    String sql = "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id = ?";
    return ctx.em().createNativeQuery(sql).setParameter(1, ownerJobId).executeUpdate();
  }

  void bindInsert(Query q, JobEntity job, Timestamp nowTs) {
    q.setParameter(1, job.getBusinessKey());
    q.setParameter(2, job.getId());
    q.setParameter(
        3,
        job.getJobType() == JobExecutionType.RECURRING ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE);
    q.setParameter(4, nowTs);
  }
}
