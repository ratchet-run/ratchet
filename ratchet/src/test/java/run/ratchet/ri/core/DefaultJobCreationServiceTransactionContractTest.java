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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobBuilder;

class DefaultJobCreationServiceTransactionContractTest {

  @Test
  void submitMethodsDeclareRequiredTransactionAttribute() throws NoSuchMethodException {
    List<Method> submitMethods =
        List.of(
            DefaultJobCreationService.class.getMethod("submit", JobBuilder.class),
            DefaultJobCreationService.class.getMethod("submit", DefaultBatchBuilder.class),
            DefaultJobCreationService.class.getMethod("submit", DefaultStreamingBatchBuilder.class),
            DefaultJobCreationService.class.getMethod("submit", DefaultRecurringJobBuilder.class));

    for (Method method : submitMethods) {
      Transactional transactional = method.getAnnotation(Transactional.class);
      assertNotNull(transactional, method + " must declare the public API TX attribute");
      assertEquals(Transactional.TxType.REQUIRED, transactional.value());
    }
  }
}
