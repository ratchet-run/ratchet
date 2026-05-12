package run.ratchet.testsuite.app;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.Test;

class DocumentStoreTestCleanupStrategyTest {

  @Test
  void documentCleanupCoversHotStateCollectionsAndMongoCounters() throws Exception {
    List<String> collections = documentCollections();

    assertTrue(
        collections.containsAll(
            List.of(
                "scheduler_job_tag", "scheduler_business_key_reservation", "scheduler_job_queue")),
        "document cleanup should cover hot state collections cleared by JPA cleanup");
    assertTrue(collections.contains("counters"), "document cleanup should keep Mongo counters");
  }

  @SuppressWarnings("unchecked")
  private static List<String> documentCollections() throws Exception {
    Field field = DocumentStoreTestCleanupStrategy.class.getDeclaredField("COLLECTIONS");
    field.setAccessible(true);
    return (List<String>) field.get(null);
  }
}
