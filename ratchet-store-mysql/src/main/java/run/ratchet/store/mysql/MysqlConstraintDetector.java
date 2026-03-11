package run.ratchet.store.mysql;

import run.ratchet.store.ConstraintDetector;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MySQL-specific constraint violation detector.
 *
 * <p>Parses MySQL error messages and codes to identify duplicate key violations and deadlocks.
 */
public class MysqlConstraintDetector implements ConstraintDetector {

  private static final Pattern CONSTRAINT_NAME_PATTERN = Pattern.compile("for key '([^']+)'");

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

  @Override
  public boolean isDeadlock(Exception e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof SQLException sql && sql.getErrorCode() == 1213) {
        return true;
      }
      String msg = current.getMessage();
      if (msg != null && msg.contains("Deadlock found")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
