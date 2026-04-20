package run.ratchet.store.mongodb;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteException;
import run.ratchet.store.ConstraintDetector;

/** MongoDB-specific constraint violation detector. */
class MongoConstraintDetector implements ConstraintDetector {

  private static final int DUPLICATE_KEY_CODE = 11000;
  private static final int WRITE_CONFLICT_CODE = 112;

  @Override
  public String constraintName(Exception e) {
    MongoWriteException mwe = findWriteException(e);
    if (mwe == null) {
      return null;
    }
    // MongoDB error message format: "...dup key: { <index_name>: ... }" or
    // "E11000 duplicate key error collection: db.coll index: <index_name> dup key: ..."
    String message = mwe.getMessage();
    if (message == null) {
      return null;
    }
    int idx = message.indexOf("index: ");
    if (idx >= 0) {
      int start = idx + "index: ".length();
      int end = message.indexOf(" ", start);
      if (end < 0) {
        end = message.length();
      }
      return message.substring(start, end);
    }
    return null;
  }

  @Override
  public boolean isDuplicateKey(Exception e) {
    MongoWriteException mwe = findWriteException(e);
    if (mwe != null && mwe.getCode() == DUPLICATE_KEY_CODE) {
      return true;
    }
    MongoCommandException mce = findCommandException(e);
    return mce != null && mce.getCode() == DUPLICATE_KEY_CODE;
  }

  @Override
  public boolean isDeadlock(Exception e) {
    MongoCommandException mce = findCommandException(e);
    if (mce != null && mce.getCode() == WRITE_CONFLICT_CODE) {
      return true;
    }
    // Also check for transient transaction errors
    if (mce != null && mce.hasErrorLabel("TransientTransactionError")) {
      return true;
    }
    MongoWriteException mwe = findWriteException(e);
    return mwe != null && mwe.getCode() == WRITE_CONFLICT_CODE;
  }

  @Override
  public boolean isTransientConnectionFailure(Exception e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof MongoSocketException
          || current instanceof MongoNotPrimaryException
          || current instanceof MongoNodeIsRecoveringException) {
        return true;
      }
      if (current instanceof MongoException mongoException
          && mongoException.hasErrorLabel("RetryableWriteError")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static MongoWriteException findWriteException(Throwable t) {
    while (t != null) {
      if (t instanceof MongoWriteException mwe) {
        return mwe;
      }
      t = t.getCause();
    }
    return null;
  }

  private static MongoCommandException findCommandException(Throwable t) {
    while (t != null) {
      if (t instanceof MongoCommandException mce) {
        return mce;
      }
      t = t.getCause();
    }
    return null;
  }
}
