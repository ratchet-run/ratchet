/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.store.entity.JobPayload;

class JobSecurityValidatorTest {

  static final AtomicInteger CLINIT_COUNTER = new AtomicInteger();
  private static final String THIS_PACKAGE = "run.ratchet.ri.security.";

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
  void emptyTargetClassThrowsSecurityException() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload = new JobPayload("", "doWork", "()V", false, List.of());

    SecurityException ex = assertThrows(SecurityException.class, () -> validator.validate(payload));

    assertTrue(ex.getMessage().contains("target class cannot be null or empty"));
  }

  @Test
  void nonPublicMethodThrowsSecurityException() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(SampleTarget.class.getName(), "secretMethod", "()V", false, List.of());
    SecurityException ex = assertThrows(SecurityException.class, () -> validator.validate(payload));
    assertEquals("Only public methods can be scheduled as jobs.", ex.getMessage());
    assertFalse(ex.getMessage().contains("private"));
    assertFalse(ex.getMessage().contains("secretMethod"));
  }

  @Test
  void nonExistentMethodThrowsSecurityExceptionWithReflectiveCause() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(SampleTarget.class.getName(), "noSuchMethod", "()V", false, List.of());

    SecurityException ex = assertThrows(SecurityException.class, () -> validator.validate(payload));

    assertInstanceOf(NoSuchMethodException.class, ex.getCause());
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

  @Test
  void recurringDispatchShimPassesEvenWhenItsOwnPackageIsNotAllowlisted() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    JobPayload payload =
        new JobPayload(
            RecurringMethodInvoker.class.getName(),
            "invoke",
            "(Ljava/lang/String;Ljava/lang/String;Z)V",
            false,
            List.of(SampleTarget.class.getName(), "doWork", false));

    assertDoesNotThrow(() -> validator.validate(payload));
  }

  @Test
  void rejectionDoesNotFireStaticInitializer() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    int before = CLINIT_COUNTER.get();
    JobPayload payload =
        new JobPayload(
            THIS_PACKAGE + "JobSecurityValidatorTest$SideEffectingTarget",
            "secretMethod",
            "()V",
            false,
            List.of());

    assertThrows(SecurityException.class, () -> validator.validate(payload));
    assertEquals(
        before, CLINIT_COUNTER.get(), "<clinit> must not fire on a class whose validation fails");
  }

  @Test
  void allowedClassWithNonPublicMethodDoesNotFireStaticInitializer() {
    JobSecurityValidator validator = validatorAllowing(THIS_PACKAGE);
    int before = CLINIT_COUNTER.get();
    JobPayload payload =
        new JobPayload(
            SideEffectingTarget.class.getName(), "secretMethod", "()V", false, List.of());

    SecurityException ex = assertThrows(SecurityException.class, () -> validator.validate(payload));

    assertEquals("Only public methods can be scheduled as jobs.", ex.getMessage());
    assertEquals(
        before,
        CLINIT_COUNTER.get(),
        "<clinit> must not fire when an allowed class fails method visibility validation");
  }

  private JobSecurityValidator validatorAllowing(String... prefixes) {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of(prefixes));
    return new JobSecurityValidator(policy);
  }

  public static class SampleTarget {
    public void doWork() {}

    public void doWorkWithArg(String arg) {}

    private void secretMethod() {}
  }

  public static class SideEffectingTarget {
    static {
      JobSecurityValidatorTest.CLINIT_COUNTER.incrementAndGet();
    }

    private void secretMethod() {}
  }
}
