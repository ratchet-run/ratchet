package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

class MongoConstraintDetectorTest {

  private static final ServerAddress SERVER_ADDRESS = new ServerAddress("localhost", 27017);

  private final MongoConstraintDetector detector = new MongoConstraintDetector();

  @Test
  void detectsSocketFailures() {
    Exception wrapped =
        new RuntimeException("wrapper", new MongoSocketOpenException("socket", SERVER_ADDRESS));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsNodeRecoveringFailures() {
    BsonDocument response = new BsonDocument("ok", new BsonInt32(0));
    Exception wrapped =
        new RuntimeException(
            "wrapper", new MongoNodeIsRecoveringException(response, SERVER_ADDRESS));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsRetryableWriteLabel() {
    MongoException mongoException = new MongoException("retryable");
    mongoException.addLabel("RetryableWriteError");

    assertTrue(detector.isTransientConnectionFailure(mongoException));
  }

  @Test
  void ignoresNonRetryableMongoExceptions() {
    assertFalse(detector.isTransientConnectionFailure(new MongoException("plain")));
  }

  @Test
  void detectsDuplicateBusinessKeyIndex() {
    Exception wrapped =
        new RuntimeException(
            "mongo",
            new MongoWriteException(
                new WriteError(
                    11000,
                    "E11000 duplicate key error collection: ratchet.scheduler_job index:"
                        + " idx_job_active_business_key dup key: { business_key: \"order-1\" }",
                    new BsonDocument()),
                SERVER_ADDRESS));

    assertTrue(detector.isDuplicateKey(wrapped));
    assertEquals("idx_job_active_business_key", detector.constraintName(wrapped));
    assertTrue(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void ignoresOtherDuplicateIndexesForBusinessKey() {
    Exception wrapped =
        new RuntimeException(
            "mongo",
            new MongoWriteException(
                new WriteError(
                    11000,
                    "E11000 duplicate key error collection: ratchet.scheduler_job index:"
                        + " idx_job_idempotency_key dup key: { idempotency_key: \"same\" }",
                    new BsonDocument()),
                SERVER_ADDRESS));

    assertTrue(detector.isDuplicateKey(wrapped));
    assertFalse(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void detectsDuplicateKeyFromCommandException() {
    MongoCommandException commandException =
        commandException(
            11000,
            "E11000 duplicate key error collection: ratchet.scheduler_job index:"
                + " idx_job_idempotency_key dup key: { idempotency_key: \"same\" }");

    assertTrue(detector.isDuplicateKey(commandException));
    assertEquals("idx_job_idempotency_key", detector.constraintName(commandException));
    assertFalse(detector.isDuplicateBusinessKey(commandException));
  }

  @Test
  void detectsWriteConflictAsDeadlock() {
    assertTrue(detector.isDeadlock(commandException(112, "WriteConflict")));
  }

  @Test
  void detectsTransientTransactionLabelAsDeadlock() {
    MongoCommandException commandException = commandException(251, "NoSuchTransaction");
    commandException.addLabel("TransientTransactionError");

    assertTrue(detector.isDeadlock(commandException));
  }

  @Test
  void detectsWriteConflictFromWriteExceptionAsDeadlock() {
    assertTrue(detector.isDeadlock(writeException(112, "WriteConflict")));
  }

  @Test
  void ignoresNonDeadlockMongoCommandException() {
    assertFalse(detector.isDeadlock(commandException(50, "ExceededTimeLimit")));
  }

  @Test
  void parsesConstraintNameAtEndOfMessage() {
    MongoWriteException exception =
        writeException(
            11000,
            "E11000 duplicate key error collection: ratchet.scheduler_job index:"
                + " idx_job_active_business_key");

    assertEquals("idx_job_active_business_key", detector.constraintName(exception));
  }

  @Test
  void returnsNullWhenConstraintNameIsAbsent() {
    assertNull(detector.constraintName(writeException(11000, "duplicate key")));
  }

  private static MongoWriteException writeException(int code, String message) {
    return new MongoWriteException(
        new WriteError(code, message, new BsonDocument()), SERVER_ADDRESS);
  }

  private static MongoCommandException commandException(int code, String message) {
    BsonDocument response =
        new BsonDocument("ok", new BsonInt32(0))
            .append("code", new BsonInt32(code))
            .append("errmsg", new org.bson.BsonString(message));
    return new MongoCommandException(response, SERVER_ADDRESS);
  }
}
