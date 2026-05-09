package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MongoClientFactoryTest {

  @Test
  void createRejectsNullConnectionStringAtFactoryBoundary() {
    NullPointerException ex =
        assertThrows(NullPointerException.class, () -> MongoClientFactory.create(null));

    assertTrue(ex.getMessage().contains("connectionString"));
  }

  @Test
  void createRejectsBlankConnectionStringAtFactoryBoundary() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> MongoClientFactory.create(" \t "));

    assertTrue(ex.getMessage().contains("connectionString"));
  }
}
