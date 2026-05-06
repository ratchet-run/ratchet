package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobContext;

class RecurringMethodValidatorTest {

  @Test
  void validate_publicNoParams_succeeds() throws Exception {
    Method method = ValidBean.class.getDeclaredMethod("publicNoParams");
    assertDoesNotThrow(() -> RecurringMethodValidator.validate(method));
  }

  @Test
  void validate_publicWithJobContext_succeeds() throws Exception {
    Method method = ValidBean.class.getDeclaredMethod("publicWithContext", JobContext.class);
    assertDoesNotThrow(() -> RecurringMethodValidator.validate(method));
  }

  @Test
  void validate_privateMethod_throws() throws Exception {
    Method method = InvalidBean.class.getDeclaredMethod("privateMethod");
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> RecurringMethodValidator.validate(method));
    assertTrue(ex.getMessage().contains("must be public"));
  }

  @Test
  void validate_tooManyParams_throws() throws Exception {
    Method method =
        InvalidBean.class.getDeclaredMethod("tooManyParams", JobContext.class, String.class);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> RecurringMethodValidator.validate(method));
    assertTrue(ex.getMessage().contains("no parameters or a single JobContext"));
  }

  @Test
  void validate_wrongParamType_throws() throws Exception {
    Method method = InvalidBean.class.getDeclaredMethod("wrongParamType", String.class);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> RecurringMethodValidator.validate(method));
    assertTrue(ex.getMessage().contains("must be of type JobContext"));
  }

  @SuppressWarnings("unused")
  public static class ValidBean {
    public void publicNoParams() {}

    public void publicWithContext(JobContext ctx) {}
  }

  @SuppressWarnings("unused")
  public static class InvalidBean {
    public void tooManyParams(JobContext ctx, String extra) {}

    public void wrongParamType(String notContext) {}

    private void privateMethod() {}
  }
}
