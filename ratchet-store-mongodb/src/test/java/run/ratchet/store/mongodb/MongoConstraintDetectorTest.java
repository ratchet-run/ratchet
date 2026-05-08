package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
