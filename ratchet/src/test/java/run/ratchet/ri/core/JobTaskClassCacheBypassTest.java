package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.spi.ClassPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the {@code CLASS_CACHE} security bypass found during peer review.
 *
 * <p>Before the fix, {@code JobTask.resolveResilienceServiceName} called {@code
 * CLASS_CACHE.computeIfAbsent(name, Class::forName)} BEFORE any call to {@code
 * validationFacade.validateSecurity}. An attacker controlling {@code payload.target()} could prime
 * the static cache with a denied class, which would then be returned unchecked on subsequent cache
 * hits.
 *
 * <p>Fix: every {@code CLASS_CACHE} load now routes through {@code JobTask.loadAllowedClass}, which
 * consults {@link ClassPolicy#isAllowed(String)} BEFORE populating or returning from the cache.
 */
class JobTaskClassCacheBypassTest {

  @BeforeEach
  void resetCache() throws Exception {
    // Clear all JobTask static caches to isolate the test.
    Method clearCaches = JobTask.class.getDeclaredMethod("clearCaches");
    clearCaches.setAccessible(true);
    clearCaches.invoke(null);
  }

  @Test
  @SuppressWarnings("unchecked")
  void loadAllowedClass_rejectsDeniedClassAndDoesNotCacheIt() throws Exception {
    ClassPolicy denyAll = className -> false;
    JobTask task = newMinimalJobTask(denyAll);

    // String is a real, loadable class — but the policy denies it.
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
    assertEquals(
        false,
        cache.containsKey("java.lang.String"),
        "Denied class must NOT be populated in CLASS_CACHE — that's the whole point of the fix");
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
    // Scenario: class was cached under a permissive policy, then policy changes to deny it.
    // Because loadAllowedClass re-checks isAllowed on every invocation, the denied class must
    // not leak out even if still present in CLASS_CACHE from a prior allow.
    Method helper = JobTask.class.getDeclaredMethod("loadAllowedClass", String.class);
    helper.setAccessible(true);

    // First load: permissive policy
    JobTask allowTask = newMinimalJobTask(className -> true);
    helper.invoke(allowTask, "java.lang.String"); // populates cache

    // Second load: restrictive policy on the same cached class
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
    // All other collaborators are null — loadAllowedClass only needs classPolicy.
    return new JobTask(null, null, null, null, null, null, null, null, null, null, classPolicy);
  }
}
