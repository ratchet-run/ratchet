package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
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
        new RuntimeException("wrapper", new MongoNodeIsRecoveringException(response, SERVER_ADDRESS));

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
}
