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
