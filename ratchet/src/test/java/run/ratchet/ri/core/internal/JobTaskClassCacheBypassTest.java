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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DefaultResultPersistenceStrategy;
import run.ratchet.ri.core.JBossLoggingJobLogger;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.ClassPolicy;

// Regression: denied class must not be cached even if loadable.
class JobTaskClassCacheBypassTest {

  @BeforeEach
  void resetCache() throws Exception {
    Method clearCaches = JobTask.class.getDeclaredMethod("clearCaches");
    clearCaches.setAccessible(true);
    clearCaches.invoke(null);
  }

  @Test
  @SuppressWarnings("unchecked")
  void loadAllowedClass_rejectsDeniedClassAndDoesNotCacheIt() throws Exception {
    ClassPolicy denyAll = className -> false;
    JobTask task = newMinimalJobTask(denyAll);

    Method helper = JobTask.class.getDeclaredMethod("loadAllowedClass", String.class);
    helper.setAccessible(true);

    SecurityException thrown =
        assertThrows(
            SecurityException.class,
            () -> {
              try {
                helper.invoke(task, "java.lang.String");
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
    assertEquals("Class java.lang.String is not allowed for job execution.", thrown.getMessage());

    Field cacheField = JobTask.class.getDeclaredField("CLASS_CACHE");
    cacheField.setAccessible(true);
    Map<String, Class<?>> cache = (Map<String, Class<?>>) cacheField.get(null);
    assertFalse(cache.containsKey("java.lang.String"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void loadAllowedClass_allowsAndCachesApprovedClass() throws Exception {
    ClassPolicy allowAll = className -> true;
    JobTask task = newMinimalJobTask(allowAll);

    Method helper = JobTask.class.getDeclaredMethod("loadAllowedClass", String.class);
    helper.setAccessible(true);

    Class<?> loaded = (Class<?>) helper.invoke(task, "java.lang.String");
    assertEquals(String.class, loaded);

    Field cacheField = JobTask.class.getDeclaredField("CLASS_CACHE");
    cacheField.setAccessible(true);
    Map<String, Class<?>> cache = (Map<String, Class<?>>) cacheField.get(null);
    assertTrue(cache.containsKey("java.lang.String"), "Approved class should be cached");
  }

  @Test
  void loadAllowedClass_rejectsPreviouslyCachedClassOnPolicyChange() throws Exception {
    Method helper = JobTask.class.getDeclaredMethod("loadAllowedClass", String.class);
    helper.setAccessible(true);

    JobTask allowTask = newMinimalJobTask(className -> true);
    helper.invoke(allowTask, "java.lang.String");

    JobTask denyTask = newMinimalJobTask(className -> false);
    SecurityException thrown =
        assertThrows(
            SecurityException.class,
            () -> {
              try {
                helper.invoke(denyTask, "java.lang.String");
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
    assertEquals("Class java.lang.String is not allowed for job execution.", thrown.getMessage());
  }

  private JobTask newMinimalJobTask(ClassPolicy classPolicy) {
    JsonbTestPayloadSerializer serializer = new JsonbTestPayloadSerializer();
    return new JobTask(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        classPolicy,
        context -> new JBossLoggingJobLogger(context.jobId(), null),
        new DefaultResultPersistenceStrategy(RatchetOptions.defaults(), serializer, null),
        null,
        serializer,
        null,
        Clock.systemUTC(),
        null);
  }
}
