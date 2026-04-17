package run.ratchet.store.mysql;

import run.ratchet.store.ConstraintDetector;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** MySQL-specific constraint violation detector. */
public class MysqlConstraintDetector implements ConstraintDetector {

  private static final Pattern CONSTRAINT_NAME_PATTERN = Pattern.compile("for key '([^']+)'");
  private static final int DEADLOCK_ERROR_CODE = 1213;
  private static final int LOCK_WAIT_TIMEOUT_ERROR_CODE = 1205;

  @Override
  public String constraintName(Exception e) {
    Throwable current = e;
    while (current != null) {
      String msg = current.getMessage();
      if (msg != null) {
        Matcher matcher = CONSTRAINT_NAME_PATTERN.matcher(msg);
        if (matcher.find()) {
          return matcher.group(1);
        }
      }
      current = current.getCause();
    }
    return null;
  }

  @Override
  public boolean isDuplicateKey(Exception e) {
    Throwable current = e;
    while (current != null) {
      String msg = current.getMessage();
      if (msg != null && msg.contains("Duplicate entry")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Returns true if the exception was raised by a unique-constraint violation on {@code
   * scheduler_business_key_reservation.business_key} (PRIMARY KEY). Use to translate the bkres
   * insert race to {@code DuplicateBusinessKeyException}.
   */
  public boolean isDuplicateBusinessKey(Exception e) {
    if (!isDuplicateKey(e)) {
      return false;
    }
    String name = constraintName(e);
    return name != null && name.contains("scheduler_business_key_reservation");
  }

  @Override
  public boolean isDeadlock(Exception e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof SQLException sql
          && (sql.getErrorCode() == DEADLOCK_ERROR_CODE
              || sql.getErrorCode() == LOCK_WAIT_TIMEOUT_ERROR_CODE)) {
        return true;
      }
      String msg = current.getMessage();
      if (msg != null
          && (msg.contains("Deadlock found") || msg.contains("Lock wait timeout exceeded"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
