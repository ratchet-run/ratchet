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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.Recurring;

class RecurringJobProcessorDiscoveryTest {

  @Test
  void discoversRecurringMethodDeclaredOnSuperclass() throws Exception {
    Method inherited = Parent.class.getDeclaredMethod("run");

    assertEquals(
        Map.of(inherited, inherited.getAnnotation(Recurring.class)), discover(Child.class));
  }

  @Test
  void annotatedOverrideIsDiscoveredOnceUsingSubclassMethod() throws Exception {
    Method override = OverridingChild.class.getDeclaredMethod("run");

    assertEquals(
        Map.of(override, override.getAnnotation(Recurring.class)), discover(OverridingChild.class));
    assertEquals("child", discover(OverridingChild.class).get(override).id());
  }

  @Test
  void skipsAnnotatedBridgeAndSyntheticMethods() throws Exception {
    // A generic parameter creates a bridge with a distinct signature, so deduplication alone
    // cannot hide a missing bridge/synthetic filter.
    assertTrue(
        Arrays.stream(GenericChild.class.getDeclaredMethods())
            .anyMatch(
                method ->
                    method.isBridge()
                        && method.isSynthetic()
                        && method.isAnnotationPresent(Recurring.class)));
    Method concrete = GenericChild.class.getDeclaredMethod("run", String.class);

    Map<Method, Recurring> discovered = discover(GenericChild.class);

    assertEquals(Map.of(concrete, concrete.getAnnotation(Recurring.class)), discovered);
    assertFalse(discovered.keySet().stream().anyMatch(Method::isBridge));
    assertFalse(discovered.keySet().stream().anyMatch(Method::isSynthetic));
  }

  @SuppressWarnings("unchecked")
  private static Map<Method, Recurring> discover(Class<?> beanClass) throws Exception {
    // Keep this regression test confined to test sources; production discovery stays private.
    Method discovery =
        RecurringJobProcessor.class.getDeclaredMethod("declaredRecurringMethods", Class.class);
    discovery.setAccessible(true);
    return (Map<Method, Recurring>) discovery.invoke(null, beanClass);
  }

  static class Parent {
    @Recurring(id = "parent", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  static class Child extends Parent {}

  static class OverridingChild extends Parent {
    @Override
    @Recurring(id = "child", cron = "0 0/10 * * * ?")
    public void run() {}
  }

  static class GenericParent<T> {
    public void run(T value) {}
  }

  static class GenericChild extends GenericParent<String> {
    @Override
    @Recurring(id = "generic", cron = "0 0/5 * * * ?")
    public void run(String value) {}
  }
}
