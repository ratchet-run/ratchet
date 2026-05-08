package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.util.BusinessKeyReservations;

final class MysqlBusinessKeyReservations {

  static final String OWNER_TABLE_QUEUE = BusinessKeyReservations.OWNER_TABLE_QUEUE;
  static final String OWNER_TABLE_RECURRING = BusinessKeyReservations.OWNER_TABLE_RECURRING;

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
    // Keep DML local: MySQL stores UUIDs as binary values and uses dialect-specific timestamps.
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
        .setParameter(2, UuidByteArrayConverter.toBytes(ownerJobId))
        .setParameter(3, ownerTable)
        .executeUpdate();
  }

  @SuppressWarnings("UnusedReturnValue")
  int deleteReservationByOwner(UUID ownerJobId) {
    // language=MySQL
    String sql = "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id = ?";
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(ownerJobId))
        .executeUpdate();
  }

  void bindInsert(Query q, JobEntity job, Timestamp nowTs) {
    q.setParameter(1, job.getBusinessKey());
    q.setParameter(2, UuidByteArrayConverter.toBytes(job.getId()));
    q.setParameter(3, BusinessKeyReservations.ownerTableFor(job.getJobType()));
    q.setParameter(4, nowTs);
  }
}
