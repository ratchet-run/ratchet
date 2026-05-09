package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JobResultTest {

  @Test
  void successWithNoValue_isSuccessTrue() {
    JobResult<Void> result = JobResult.success(null);

    assertTrue(result.isSuccess());
    assertFalse(result.isFailure());
    assertFalse(result.hasValue());
    assertFalse(result.hasError());
    assertNull(result.getValue());
    assertNull(result.getError());
    assertNull(result.getException());
  }

  @Test
  void successWithValue_returnsValue() {
    JobResult<Integer> result = JobResult.success(42);

    assertTrue(result.isSuccess());
    assertFalse(result.isFailure());
    assertTrue(result.hasValue());
    assertEquals(42, result.getValue());
  }

  @Test
  void failureWithErrorMessage_isFailure() {
    JobResult<Void> result = JobResult.failure("something went wrong", null);

    assertTrue(result.isFailure());
    assertFalse(result.isSuccess());
    assertTrue(result.hasError());
    assertEquals("something went wrong", result.getError());
    assertNull(result.getException());
    assertNull(result.getValue());
  }

  @Test
  void failureWithErrorAndException_preservesException() {
    RuntimeException cause = new RuntimeException("boom");
    JobResult<Void> result = JobResult.failure("operation failed", cause);

    assertTrue(result.isFailure());
    assertTrue(result.hasError());
    assertEquals("operation failed", result.getError());
    assertSame(cause, result.getException());
  }

  @Test
  void failureWithNullErrorAndExceptionIsStillFailure() {
    JobResult<Void> result = JobResult.failure(null, null);

    assertTrue(result.isFailure());
    assertFalse(result.isSuccess());
    assertFalse(result.hasError());
    assertNull(result.getError());
    assertNull(result.getException());
  }

  @Test
  void writeReplaceSanitizesNonSerializableThrowable() throws Exception {
    NonSerializableException original = new NonSerializableException("boom");
    original.setStackTrace(new StackTraceElement[] {new StackTraceElement("C", "m", "C.java", 7)});
    JobResult<Void> result = JobResult.failure("operation failed", original);

    JobResult<?> roundTripped = roundTrip(result);

    assertEquals("operation failed", roundTripped.getError());
    assertInstanceOf(RuntimeException.class, roundTripped.getException());
    assertEquals(
        NonSerializableException.class.getName() + ": boom",
        roundTripped.getException().getMessage());
    assertArrayEquals(original.getStackTrace(), roundTripped.getException().getStackTrace());
  }

  @Test
  void of_allFieldsPopulated() {
    Instant start = Instant.parse("2025-01-01T00:00:00Z");
    Instant end = Instant.parse("2025-01-01T00:01:00Z");
    Map<String, Object> meta = Map.of("key", "value");

    JobResult<String> result = JobResult.of(true, "done", null, null, 60000L, start, end, meta);

    assertTrue(result.isSuccess());
    assertEquals("done", result.getValue());
    assertNull(result.getError());
    assertNull(result.getException());
    assertEquals(60000L, result.getExecutionTimeMs());
    assertEquals(start, result.getStartTime());
    assertEquals(end, result.getEndTime());
    assertEquals(meta, result.getMetadata());
  }

  @Test
  void getMetadataByKey_returnsValueWhenPresent() {
    Map<String, Object> meta = Map.of("count", 100, "status", "ok");
    JobResult<Void> result = JobResult.of(true, null, null, null, null, null, null, meta);

    assertEquals(100, result.getMetadata("count"));
    assertEquals("ok", result.getMetadata("status"));
  }

  @Test
  void getMetadataByKey_returnsNullWhenKeyMissing() {
    Map<String, Object> meta = Map.of("count", 100);
    JobResult<Void> result = JobResult.of(true, null, null, null, null, null, null, meta);

    assertNull(result.getMetadata("missing"));
  }

  @Test
  void getMetadataByKey_returnsNullWhenMetadataMapIsNull() {
    JobResult<Void> result = JobResult.success(null);

    assertNull(result.getMetadata("anything"));
  }

  @Test
  void getMetadataWithDefault_returnsDefaultWhenKeyMissing() {
    Map<String, Object> meta = Map.of("count", 100);
    JobResult<Void> result = JobResult.of(true, null, null, null, null, null, null, meta);

    assertEquals("fallback", result.getMetadata("missing", "fallback"));
  }

  @Test
  void getMetadataWithDefault_returnsDefaultWhenMetadataNull() {
    JobResult<Void> result = JobResult.success(null);

    assertEquals(42, result.getMetadata("key", 42));
  }

  @Test
  void getMetadataWithDefault_returnsValueWhenPresent() {
    Map<String, Object> meta = Map.of("level", "high");
    JobResult<Void> result = JobResult.of(true, null, null, null, null, null, null, meta);

    assertEquals("high", result.getMetadata("level", "low"));
  }

  @Test
  void getExecutionTimeMsOrZero_returnsZeroWhenNull() {
    JobResult<Void> result = JobResult.success(null);

    assertEquals(0L, result.getExecutionTimeMsOrZero());
  }

  @Test
  void getExecutionTimeMsOrZero_returnsValueWhenPresent() {
    JobResult<Void> result = JobResult.of(true, null, null, null, 5000L, null, null, null);

    assertEquals(5000L, result.getExecutionTimeMsOrZero());
  }

  @Test
  void equals_sameFieldsAreEqual() {
    Instant now = Instant.now();
    JobResult<String> a = JobResult.of(true, "v", null, null, 100L, now, now, null);
    JobResult<String> b = JobResult.of(true, "v", null, null, 100L, now, now, null);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void equals_differentFieldsAreNotEqual() {
    JobResult<String> success = JobResult.success("ok");
    JobResult<String> failure = JobResult.failure("err", null);

    assertNotEquals(success, failure);
  }

  private static JobResult<?> roundTrip(JobResult<?> result) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(result);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (JobResult<?>) in.readObject();
    }
  }

  private static final class NonSerializableException extends Exception {

    @Serial private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private final Object nonSerializable = new Object();

    private NonSerializableException(String message) {
      super(message);
    }
  }
}
