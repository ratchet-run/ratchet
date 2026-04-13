package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.spi.ClassPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    Throwable thrown =
        assertThrows(
            Throwable.class,
            () -> {
              try {
                helper.invoke(task, "java.lang.String");
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
            });
    assertTrue(
        thrown instanceof SecurityException,
        "Denied class must throw SecurityException, got: " + thrown);

    Field cacheField = JobTask.class.getDeclaredField("CLASS_CACHE");
    cacheField.setAccessible(true);
    Map<String, Class<?>> cache = (ConcurrentHashMap<String, Class<?>>) cacheField.get(null);
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
    Map<String, Class<?>> cache = (ConcurrentHashMap<String, Class<?>>) cacheField.get(null);
    assertTrue(cache.containsKey("java.lang.String"), "Approved class should be cached");
  }

  @Test
  void loadAllowedClass_rejectsPreviouslyCachedClassOnPolicyChange() throws Exception {
    Method helper = JobTask.class.getDeclaredMethod("loadAllowedClass", String.class);
    helper.setAccessible(true);

    JobTask allowTask = newMinimalJobTask(className -> true);
    helper.invoke(allowTask, "java.lang.String");

    JobTask denyTask = newMinimalJobTask(className -> false);
    Throwable thrown =
        assertThrows(
            Throwable.class,
            () -> {
              try {
                helper.invoke(denyTask, "java.lang.String");
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
            });
    assertTrue(
        thrown instanceof SecurityException,
        "Cache hit must re-validate policy; denied class must throw even if cached");
  }

  private JobTask newMinimalJobTask(ClassPolicy classPolicy) {
    return new JobTask(null, null, null, null, null, null, null, null, null, null, classPolicy);
  }
}
