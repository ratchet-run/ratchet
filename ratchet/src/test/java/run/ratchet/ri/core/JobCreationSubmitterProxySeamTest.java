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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSubmitter;

class JobCreationSubmitterProxySeamTest {

  @Test
  void creationServiceExposesOnlyPublicSubmissionContractsToConsumers() {
    List<Class<?>> submissionContracts =
        List.of(
            JobSubmitter.class,
            BatchSubmitter.class,
            StreamingBatchSubmitter.class,
            RecurringJobSubmitter.class);

    for (Class<?> contract : submissionContracts) {
      assertTrue(contract.isInterface());
      assertTrue(Modifier.isPublic(contract.getModifiers()));
      assertTrue(contract.isAssignableFrom(DefaultJobCreationService.class));
    }

    assertFalse(Modifier.isFinal(DefaultJobCreationService.class.getModifiers()));
  }

  @Test
  void consumersHaveNoConcreteCreationServiceInjectionEdge() {
    assertNoConcreteCreationServiceEdge(DefaultInvocationSubmissionService.class);
    assertNoConcreteCreationServiceEdge(DefaultJobSchedulerService.class);
  }

  @Test
  void publicSubmissionContractsCanBeExposedByOneJdkProxy() {
    Object proxy =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {
              JobSubmitter.class,
              BatchSubmitter.class,
              StreamingBatchSubmitter.class,
              RecurringJobSubmitter.class
            },
            (ignored, method, arguments) -> null);

    assertTrue(proxy instanceof JobSubmitter);
    assertTrue(proxy instanceof BatchSubmitter);
    assertTrue(proxy instanceof StreamingBatchSubmitter);
    assertTrue(proxy instanceof RecurringJobSubmitter);
    ((BatchSubmitter) proxy).submit(null);
    ((StreamingBatchSubmitter) proxy).submit(null);
    ((RecurringJobSubmitter) proxy).submit(null);
  }

  private static void assertNoConcreteCreationServiceEdge(Class<?> consumer) {
    for (Field field : consumer.getDeclaredFields()) {
      assertFalse(field.getType() == DefaultJobCreationService.class);
    }
    for (Constructor<?> constructor : consumer.getDeclaredConstructors()) {
      for (Class<?> parameterType : constructor.getParameterTypes()) {
        assertFalse(parameterType == DefaultJobCreationService.class);
      }
    }
  }
}
