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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.entity.JobPayload;

class JobPayloadInputValidatorTest {

  private final JobPayloadInputValidator validator = new JobPayloadInputValidator();

  @AfterEach
  void discardPreparedSerializations() {
    new JobPayloadConverter().discardAllPreparedSerializations();
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
  void argumentTypeMismatchReportsError() {
    JobPayload payload =
        new JobPayload(Target.class.getName(), "add", "(II)V", false, List.of(1, "two"));

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("Argument value at position 1"));
    assertTrue(ex.getMessage().contains(String.class.getName()));
    assertTrue(ex.getMessage().contains("int"));
  }

  @Test
  void argumentCountMismatchReportsError() {
    JobPayload payload = new JobPayload(Target.class.getName(), "add", "(II)V", false, List.of(1));

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("Argument count mismatch"));
    assertTrue(ex.getMessage().contains("expected 2"));
    assertTrue(ex.getMessage().contains("payload has 1"));
  }

  @Test
  void nullArgsReportsClearValidationError() {
    JobPayload payload =
        new JobPayload(Target.class.getName(), "greet", "(Ljava/lang/String;)V", false, null);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("Arguments cannot be null"));
  }

  @Test
  void malformedBaseFieldsAccumulateBeforeSignatureValidation() {
    JobPayload payload = new JobPayload(null, null, null, false, null);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("Target class cannot be null or empty"));
    assertTrue(ex.getMessage().contains("Method name cannot be null or empty"));
    assertTrue(ex.getMessage().contains("Method descriptor cannot be null or empty"));
    assertFalse(ex.getMessage().contains("Arguments cannot be null"));
  }

  @Test
  void privateMethodReportsVisibilityError() {
    JobPayload payload = new JobPayload(Target.class.getName(), "hidden", "()V", false, List.of());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("is private"));
    assertTrue(ex.getMessage().contains("only public methods can be scheduled"));
  }

  @Test
  void protectedMethodReportsVisibilityError() {
    JobPayload payload = new JobPayload(Target.class.getName(), "guarded", "()V", false, List.of());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("is protected"));
    assertTrue(ex.getMessage().contains("only public methods can be scheduled"));
  }

  @Test
  void packagePrivateMethodReportsVisibilityError() {
    JobPayload payload = new JobPayload(Target.class.getName(), "local", "()V", false, List.of());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("is package-private"));
    assertTrue(ex.getMessage().contains("only public methods can be scheduled"));
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
  void targetClassLinkageFailureIsReportedAsValidationError() {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new LinkageFailureClassLoader(original));
    try {
      JobPayload payload = new JobPayload("example.Broken", "run", "()V", false, List.of());

      IllegalArgumentException ex =
          assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

      assertTrue(ex.getMessage().contains("Cannot load target class example.Broken"));
      assertTrue(ex.getMessage().contains("java.lang.NoClassDefFoundError"));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void invalidTargetClassNameRejectedBeforeClassLoading() {
    JobPayload payload = new JobPayload("java.lang..Runtime", "exec", "()V", false, List.of());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));

    assertTrue(ex.getMessage().contains("Target class name contains invalid characters"));
    assertFalse(ex.getMessage().contains("Target class not found"));
  }

  @Test
  void nonExistentMethodReportsError() {
    JobPayload payload =
        new JobPayload(Target.class.getName(), "noSuchMethod", "()V", false, List.of());
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateAtCreation(payload));
    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void signatureValidationExceptionIsReportedAsValidationError() throws Exception {
    JobPayload payload = new JobPayload(Target.class.getName(), "run", "()V", false, List.of());
    List<String> errors = new ArrayList<>();
    Method validateMethodSignature =
        JobPayloadInputValidator.class.getDeclaredMethod(
            "validateMethodSignature", Class.class, JobPayload.class, List.class);
    validateMethodSignature.setAccessible(true);

    validateMethodSignature.invoke(validator, null, payload, errors);

    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Failed to validate method signature"));
  }

  public static class Target {
    public void run() {}

    public void greet(String name) {}

    public void add(int a, int b) {}

    private void hidden() {}

    protected void guarded() {}

    void local() {}
  }

  private static final class LinkageFailureClassLoader extends ClassLoader {
    private LinkageFailureClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if ("example.Broken".equals(name)) {
        throw new NoClassDefFoundError("missing dependency");
      }
      return super.loadClass(name, resolve);
    }
  }
}
