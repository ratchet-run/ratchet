package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.*;

import run.ratchet.store.entity.JobPayload;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JobSecurityValidatorTest {

  private static final String THIS_PACKAGE = "run.ratchet.ri.security.";

  public static class SampleTarget {
    public void doWork() {}

    public void doWorkWithArg(String arg) {}

    private void secretMethod() {}
  }

  private JobSecurityValidator validatorAllowing(String... prefixes) {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of(prefixes));
    return new JobSecurityValidator(policy);
  }

  @Test
  void allowedClassAndPublicMethodPasses() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(SampleTarget.class.getName(), "doWork", "()V", false, List.of());
    assertDoesNotThrow(() -> validator.validate(payload));
  }

  @Test
  void disallowedClassThrowsSecurityException() {
    JobSecurityValidator validator = validatorAllowing("com.trusted.");
    JobPayload payload =
        new JobPayload(SampleTarget.class.getName(), "doWork", "()V", false, List.of());
    assertThrows(SecurityException.class, () -> validator.validate(payload));
  }

  @Test
  void nullPayloadThrowsSecurityException() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    assertThrows(SecurityException.class, () -> validator.validate(null));
  }

  @Test
  void nullTargetClassThrowsSecurityException() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload = new JobPayload(null, "doWork", "()V", false, List.of());
    assertThrows(SecurityException.class, () -> validator.validate(payload));
  }

  @Test
  void nonPublicMethodThrowsSecurityException() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(SampleTarget.class.getName(), "secretMethod", "()V", false, List.of());
    SecurityException ex = assertThrows(SecurityException.class, () -> validator.validate(payload));
    assertTrue(ex.getMessage().contains("private"));
  }

  @Test
  void nonExistentMethodThrows() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(SampleTarget.class.getName(), "noSuchMethod", "()V", false, List.of());
    assertThrows(NoSuchMethodException.class, () -> validator.validate(payload));
  }

  @Test
  void publicMethodWithArgInAllowedPackagePasses() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(
            SampleTarget.class.getName(),
            "doWorkWithArg",
            "(Ljava/lang/String;)V",
            false,
            List.of("hello"));
    assertDoesNotThrow(() -> validator.validate(payload));
  }
}
