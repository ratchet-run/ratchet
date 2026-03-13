package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.*;

import run.ratchet.store.entity.JobPayload;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobPayloadInputValidatorTest {

  private final JobPayloadInputValidator validator = new JobPayloadInputValidator();

  /** Public target class used by validation tests. */
  public static class Target {
    public void run() {}

    public void greet(String name) {}

    public void add(int a, int b) {}
  }

  @Test
  void nullPayloadThrows() {
    assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(null));
  }

  @Test
  void nullTargetClassReportsError() {
    JobPayload payload = new JobPayload(null, "run", "()V", false, List.of());
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("Target class"));
  }

  @Test
  void nullMethodNameReportsError() {
    JobPayload payload = new JobPayload(Target.class.getName(), null, "()V", false, List.of());
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("Method name"));
  }

  @Test
  void nullMethodDescriptorReportsError() {
    JobPayload payload = new JobPayload(Target.class.getName(), "run", null, false, List.of());
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("Method descriptor"));
  }

  @Test
  void validPayloadPasses() {
    JobPayload payload = new JobPayload(Target.class.getName(), "run", "()V", false, List.of());
    assertDoesNotThrow(() -> validator.validateAtCreation(payload));
  }

  @Test
  void validPayloadWithArgPasses() {
    JobPayload payload =
        new JobPayload(
            Target.class.getName(), "greet", "(Ljava/lang/String;)V", false, List.of("Alice"));
    assertDoesNotThrow(() -> validator.validateAtCreation(payload));
  }

  @Test
  void nonExistentClassReportsError() {
    JobPayload payload =
        new JobPayload("com.nonexistent.NoSuchClass", "run", "()V", false, List.of());
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("Target class not found"));
  }

  @Test
  void nonExistentMethodReportsError() {
    JobPayload payload =
        new JobPayload(Target.class.getName(), "noSuchMethod", "()V", false, List.of());
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("not found"));
  }
}
