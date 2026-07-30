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
package run.ratchet.ri.core.internal;

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
  void validate_staticMethod_throws() throws Exception {
    Method method = InvalidBean.class.getDeclaredMethod("staticMethod");
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> RecurringMethodValidator.validate(method));
    assertTrue(ex.getMessage().contains("must not be static"));
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

    public static void staticMethod() {}
  }
}
