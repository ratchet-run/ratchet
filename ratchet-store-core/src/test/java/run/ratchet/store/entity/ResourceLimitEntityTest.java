package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class ResourceLimitEntityTest {

  @Test
  void defaultRetryDelayUsesNamedConstant() {
    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS, new ResourceLimitEntity().getRetryDelayMs());
  }

  @Test
  void equalityUsesResourceNameIdentity() {
    ResourceLimitEntity first = limit("database", 10);
    ResourceLimitEntity second = limit("database", 20);
    ResourceLimitEntity different = limit("api", 10);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, different);
  }

  private static ResourceLimitEntity limit(String resourceName, int maxConcurrent) {
    ResourceLimitEntity limit = new ResourceLimitEntity();
    limit.setResourceName(resourceName);
    limit.setMaxConcurrent(maxConcurrent);
    return limit;
  }
}
