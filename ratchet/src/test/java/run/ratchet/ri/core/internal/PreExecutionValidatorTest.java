package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobPayload;

class PreExecutionValidatorTest {

  @Test
  void validateSecurityDoesNotExposeReflectiveCheckedExceptions() throws NoSuchMethodException {
    Method method = PreExecutionValidator.class.getMethod("validateSecurity", JobPayload.class);

    assertEquals(0, method.getExceptionTypes().length);
  }
}
